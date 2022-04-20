{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Copy
-- Description : Copying data from one duct to another
-- Copyright   : (c) Brian Hurt, 2022
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- = Warning
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
-- = Purpose
--
-- With most of the actual routing logic moved into the Duct STM actions,
-- a few common functions take care of most of the simple threads.
--
module Data.Conduit.Parallel.Internal.Copy(
    copyThread,
    discardThread,
    spamThread
) where

    import           Control.Monad.IO.Class
    import           Control.Monad.STM
    import           Control.Monad.Trans.Maybe
    import           Data.Void

    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn

    -- | Wrapper function for running STM Actions in a MaybeT.
    doAct :: forall x .
                STM (Maybe x)
                -> MaybeT IO x
    doAct act = do
        -- Blocked indefinitely on STM is the same as
        -- closing the duct.
        mx :: Maybe x <- MaybeT . safeAtomically $ act

        -- If the duct is closed, exit.  Otherwise, return
        -- the value.
        MaybeT $ pure mx

    -- | Run a loop
    --
    -- We do multiple loops in this module.  They all have the type
    -- @MaybeT IO Void@.  The @Void@ return type means the loop will
    -- only exit when an STM action returns @Nothing@ (i.e. we are
    -- reading from or writing to a channel that is closed).
    --
    runLoop :: forall m .
                MonadIO m
                => MaybeT IO Void
                -> m ()
    runLoop loop = liftIO $ do
        r :: Maybe Void <- runMaybeT loop
        case r of
            Just v  -> absurd v
            Nothing -> pure ()

    -- | Copy values from a read duct to a write duct unchanged.
    --
    -- This is the main code for shim threads.
    copyThread :: forall m a .
                    MonadIO m
                    => ReadDuct m a
                    -> WriteDuct m a
                    -> WorkerThread m ()
    copyThread wrd wwd = do
        rd <- getReadDuct wrd
        wd <- getWriteDuct wwd
        let loop :: MaybeT IO Void
            loop = do
                a :: a <- doAct rd
                doAct $ wd a
                loop
        runLoop loop

    -- | Read and discard all elements from a Read duct.
    --
    -- Used when we have a duct that we don't know is already closed,
    -- but that we want to discard the elements from.
    --
    discardThread :: forall m a .
                        MonadIO m
                        => ReadDuct m a
                        -> WorkerThread m ()
    discardThread wrd = do
        rd <- getReadDuct wrd
        let loop :: MaybeT IO Void
            loop = do
                _ :: a <- doAct rd
                loop
        runLoop loop

    -- | Continually write a constant value to a write duct.
    spamThread :: forall m a .
                    MonadIO m
                    => WriteDuct m a
                    -> a
                    -> WorkerThread m ()
    spamThread wwd a = do
        wd <- getWriteDuct wwd
        let loop :: MaybeT IO Void
            loop = do
                doAct $ wd a
                loop
        runLoop loop

