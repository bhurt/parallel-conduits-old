{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct
-- Description : Parallel Conduit Ducts
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
-- This module introduces Ducts, which are like MVars but have an additional
-- Closed state.  In addition, we split a Duct into two endpoints- a read
-- endpoint and a write endpoint.  This makes Ducts more like unix pipes
-- in this way.
--
-- = Motivation
-- 
-- The problem with MVars is that there is no easy way to signal the
-- writers of the duct that the readers have all exited.  Previous attempts
-- of this library tried to use MVars with a side channel IORef to signal
-- that the readers had exited.  But this proved clunky.
--
-- This implementation uses STM.  The advantage of STM is that we can easily
-- define our own "MVar-plus" type around a single TVar holding a
-- @DuctState@.  Even more so, we can build up a new Duct endpoint
-- ontop of multiple other duct endpoints- see
-- `Data.Conduit.Parallel.Internal.Duct.Write.writeTuple`,
-- `Data.Conduit.Parallel.Internal.Duct.Write.writeEither`,
-- etc. for examples.
--
-- The problem with STM is that it does give rise to the "thundering herd"
-- problem- writing a single TVar and wake up many threads.  This is one
-- place where MVars shine: they guarantee a single wakeup.  However, in
-- the specific use case of parallel conduits, there is a simple solution.
--
-- We simply add "shim threads" in those places where the thundering herd
-- is likely to be a problem.  A shim thread either reads from one duct
-- and writes to multiple ducts, or reads from multiple ducts and writes
-- to one duct.
-- 
-- This allows us to guarantee that there is only ever a single thread on
-- either end of a duct (even though a given thread may be on the end of
-- multiple ducts).  In this library, it's not a problem, but this sharp
-- corner of ducts is why they will never be made into their own library.
--
module Data.Conduit.Parallel.Internal.Duct where

    import           Control.Applicative
    import           Control.Concurrent.STM
    import           Control.Monad.IO.Class
    import           Control.Monad.Trans.Maybe
    import           Control.Selective
    import           Data.Functor.Contravariant
    import           Data.Functor.Contravariant.Divisible
    import           Data.These

    import           Data.Conduit.Parallel.Internal.Duct.Utils
    import           Data.Conduit.Parallel.Internal.Spawn

    -- | A duct, which is the pair of a read endpoint and a write endpoint.
    --
    -- This structure could just be a tuple, but from experience,
    -- which ever order I return the two endpoints in, I'll screw
    -- it up about half the time.
    --
    data Duct m a =
        Duct {
            -- | Get the read endpoint of the duct.
            getReadEndpoint  :: ReadDuct m a,

            -- | Get the write endpoint of the duct.
            getWriteEndpoint :: WriteDuct m a }


    -- | The read endpoint of a duct.
    --
    -- This is an STM action available as a resource in a worker thread.
    -- The STM action resource will behave the following way:
    -- 
    --  * If the duct is not closed and contains at least one value,
    --      that value will be removed from the duct and returned from
    --      the STM action in a @Just@.
    --  * If the duct is not closed but does not contain even one value,
    --      the STM action will retry.
    --  * If the duct is closed, the STM action will return @Nothing@.
    --
    -- Once the worker thread exits (and the STM action is released)
    -- the duct will be closed.
    --
    newtype ReadDuct m a =
        ReadDuct {
            getReadDuct :: WorkerThread m (STM (Maybe a)) }

    instance Functor m => Functor (ReadDuct m) where
        fmap f rd = ReadDuct $ fmap (fmap f) <$> getReadDuct rd

    instance MonadIO m => Applicative (ReadDuct m) where
        pure :: forall a . a -> ReadDuct m a
        pure a = ReadDuct $ pure (pure (Just a))

        liftA2 :: forall a b c .
                    (a -> b -> c)
                    -> ReadDuct m a
                    -> ReadDuct m b
                    -> ReadDuct m c
        liftA2 f da db = ReadDuct go
            where
                go :: WorkerThread m (STM (Maybe c))
                go = do
                    ra :: STM (Maybe a) <- getReadDuct da
                    rb :: STM (Maybe b) <- getReadDuct db
                    enforceClosed $ \g -> g (doRead ra rb)

                doRead :: STM (Maybe a)
                            -> STM (Maybe b)
                            -> STM (Maybe c)
                doRead ra rb = catchClosedDuct $ do
                    a <- throwClosed ra
                    b <- throwClosed rb
                    pure . Just $ f a b

    -- | Selective gives us a whole slew of useful functions.
    --
    -- See <https://hackage.haskell.org/package/selective-0.5/docs/Control-Selective.html>
    instance MonadIO m => Selective (ReadDuct m) where
        select :: forall a b .
                    ReadDuct m (Either a b)
                    -> ReadDuct m (a -> b)
                    -> ReadDuct m b
        select de df = ReadDuct go
            where
                go :: WorkerThread m (STM (Maybe b))
                go = do
                    re :: STM (Maybe (Either a b)) <- getReadDuct de
                    rf :: STM (Maybe (a -> b)) <- getReadDuct df
                    enforceClosed $ \g -> g (doRead re rf)

                doRead :: STM (Maybe (Either a b))
                            -> STM (Maybe (a -> b))
                            -> STM (Maybe b)
                doRead re rf = runMaybeT $ do
                    e <- MaybeT re
                    case e of
                        Left a -> do
                            f <- MaybeT rf
                            pure $ f a
                        Right b -> pure b

    -- | The write endpoint of a duct.
    --
    -- This is an STM action available as a resource in a worker thread.
    -- The STM action resource will behave the following way:
    -- 
    --  * If the duct is not closed and can take at least one more
    --      value, the value is added to the duct and @Just ()@ is
    --      returned from the STM action.
    --  * If the duct is not closed but can not take even one more
    --      value, then the STM action will retry.
    --  * If the duct is closed, the STM action will return @Nothing@.
    --
    -- Once the worker thread exits (and the STM action is released)
    -- the duct will be closed.
    --
    newtype WriteDuct m a =
        WriteDuct {
            getWriteDuct :: WorkerThread m (a -> STM (Maybe ())) }

    instance Functor m => Contravariant (WriteDuct m) where
        contramap f wd = WriteDuct $ (. f) <$> getWriteDuct wd

    writeFilter :: forall m a .
                    MonadIO m
                    => WriteDuct m a
                    -> WriteDuct m (Maybe a)
    writeFilter da = WriteDuct go
        where
            go :: WorkerThread m (Maybe a -> STM (Maybe ()))
            go = do
                wa <- getWriteDuct da
                enforceClosed $ \f -> \a -> f (doWrite wa a)

            doWrite :: (a -> STM (Maybe ()))
                        -> Maybe a
                        -> STM (Maybe ())
            doWrite _  Nothing  = pure $ Just ()
            doWrite wa (Just a) = wa a

    writeThose :: forall m a b c .
                    MonadIO m
                    => (a -> These b c)
                    -> WriteDuct m b
                    -> WriteDuct m c
                    -> WriteDuct m a
    writeThose f db dc = WriteDuct go
        where
            go :: WorkerThread m (a -> STM (Maybe ()))
            go = do
                wb :: (b -> STM (Maybe ())) <- getWriteDuct db
                wc :: (c -> STM (Maybe ())) <- getWriteDuct dc
                enforceClosed $ \g -> \a -> g (doWrite wb wc a)

            doWrite :: (b -> STM (Maybe ()))
                        -> (c -> STM (Maybe ()))
                        -> a
                        -> STM (Maybe ())
            doWrite wb wc a = do
                case (f a) of
                    This  b   -> wb b
                    That    c -> wc c
                    These b c -> catchClosedDuct $ do
                                    throwClosed $ wb b
                                    throwClosed $ wc c
                                    pure $ Just ()

    instance MonadIO m => Divisible  (WriteDuct m) where
        divide f = writeThose (\a -> let (b, c) = f a in These b c)
        conquer = WriteDuct (pure (const (pure (Just ()))))
        

    instance MonadIO m => Decidable (WriteDuct m) where
        lose _ = WriteDuct (pure (const (pure Nothing)))
        choose f = writeThose go
            where
                go a = case (f a) of
                            Left b  -> This b
                            Right c -> That c
