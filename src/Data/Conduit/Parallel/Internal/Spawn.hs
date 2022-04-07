{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Spawn
-- Description : Thread spawning and thread management
-- Copyright   : (c) Brian Hurt, 2022
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
-- This module handles thread spawning and thread maangement.  Our
-- overall structure is that we have two different kinds of threads:
--
-- [@Control Thread@] Spawns one or more worker threads and then
-- waits for them to complete.  Generally there is only one
-- @Control Thread@.
-- [@Worker Thread@] A Thread which actually does work.  This includes
-- both threads doing user-defined work (running ordinary Conduits,
-- etc.) and shim threads created by the library to avoid the thundering
-- herd problem (see [Data.Conduit.Parallel.Internal.Duct](Data-Conduit-Parallel-Internal-Duct.html) for more information).
--
-- We use Codensity (from the Kan-Extensions library) instead of ContT
-- because the forall's are in a nicer place.  But they're essentially
-- the same monad.  Good resource on the ContT monad:
-- <https://ro-che.info/articles/2019-06-07-why-use-contt>
--
module Data.Conduit.Parallel.Internal.Spawn where

    import           Control.Exception       (BlockedIndefinitelyOnSTM(..))
    import           Control.Monad.Codensity
    import           Control.Monad.Trans
    import           UnliftIO

    -- | A computation to be performed in a worker thread.
    newtype WorkerThread m a = WorkerThread {
                                    runWorkerThread :: Codensity m a }
        deriving (Functor, Applicative, Monad, MonadTrans, MonadIO)

    -- | A computation to be performed in the control thread.
    newtype ControlThread m a = ControlThread { 
                                    runControlThread :: Codensity m a }
        deriving (Functor, Applicative, Monad, MonadTrans, MonadIO)

    -- | Spawn a new worker thread in the control thread.
    --
    -- This is how you execute @WorkerThread@ monads.
    --
    -- If the worker thread throws an exception, this is propagated up
    -- to the control thread.
    --
    -- What is returned is a monadic action that waits on the spawned
    -- worker thread to complete.  This will be build up into a single
    -- large monadic action that waits on all worker threads to
    -- complete.
    spawn :: forall m a . MonadUnliftIO m
                            => WorkerThread m a
                            -> ControlThread m (m a)
    spawn workerThread = ControlThread $ do
        asy <- Codensity $
                    withAsync (runCodensity (runWorkerThread workerThread) pure)
        lift $ link asy
        pure $ wait asy

    -- | Acquire, and ensure the release, of a resource in a worker thread.
    --
    -- This is just a wrapper around Control.Exception's @bracket@ function
    -- for worker threads.
    workerThreadBracket :: forall a b m .
                    MonadUnliftIO m
                    => m a
                    -> (a -> m b)
                    -> WorkerThread m a
    workerThreadBracket make destroy =
        WorkerThread $ Codensity $ bracket make destroy
    -- Note, the definition:
    --  workerThreadBracket = WorkerThread . Codensity . bracket
    -- doesn't type check.

    -- | Acquire, and ensure the release, of a resource in the control thread.
    --
    -- This is just a wrapper around Control.Exception's @bracket@ function
    -- for the control thread.
    controlThreadBracket :: forall a b m .
                    MonadUnliftIO m
                    => m a
                    -> (a -> m b)
                    -> ControlThread m a
    controlThreadBracket make destroy =
        ControlThread $ Codensity $ bracket make destroy
    -- Note, the definition:
    --  maintreadBracket = ControlThread . Codensity . bracket
    -- doesn't type check.

    -- | Run the main control thread.
    --
    -- This is how @ControlThread@ values are executed.
    --
    -- This is just a wrapper around @runCodensity@.
    runControl :: ControlThread m a -> (a -> m b) -> m b
    runControl mt = runCodensity $ runControlThread mt
    -- Note, the definition:
    --  runControl = runCodensity . runControlThread
    -- doesn't type check.


    -- | Run an STM action both atomically and safely.
    --
    -- Just a wrapper around STM's @atomically@, but we catch and handle
    -- @BlockedIndefinitelyOnSTM@ exceptions.  On exception, we return
    -- @Nothing@.  This prevents us from accidentally throwing exceptions
    -- when we don't need to.
    safeAtomically :: forall m a .
                        MonadUnliftIO m
                        => STM a
                        -> m (Maybe a)
    safeAtomically stm = do
        r <- try $ atomically $ stm
        case r of
            Left  BlockedIndefinitelyOnSTM -> pure Nothing
            Right a                        -> pure $ Just a

