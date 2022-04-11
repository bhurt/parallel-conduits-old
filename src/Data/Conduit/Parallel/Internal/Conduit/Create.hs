{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Conduit.Create
-- Description : Create ParConduit segments
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
-- Home for the different ways to create "leaf" parallel conduit segments,
-- that is segments that spawn only a single thread.
--

module Data.Conduit.Parallel.Internal.Conduit.Create where

    import           Control.DeepSeq
    import           Control.Exception       (evaluate)
    import           Control.Monad           (join)
    import           Control.Monad.IO.Class
    import           Control.Monad.IO.Unlift
    import           Control.Monad.STM
    import           Control.Monad.Trans     (lift)
    import           Data.Conduit

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn

    -- | Make a conduit from a raw action.
    --
    -- This is the most basic way to implement a conduit.
    basicParConduit :: forall m r i o .
                        (MonadUnliftIO m
                        , NFData o)
                        => (m (Maybe i)
                            -> (o -> m (Maybe ()))
                            -> m r)
                        -> ParConduit m r i o
    basicParConduit act = ParConduit $ \rd wd -> spawn (worker rd wd)
        where
            worker :: ReadDuct Simple m i
                        -> WriteDuct Simple m o
                        -> WorkerThread m r
            worker rd wd = do
                r <- getReadDuct rd
                w <- getWriteDuct wd
                lift $ act (doRead r) (doWrite w)

            doRead :: STM (Maybe i) -> m (Maybe i)
            doRead stm = join <$> safeAtomically stm

            doWrite :: (o -> STM (Maybe ())) -> o -> m (Maybe ())
            doWrite stm o' = do
                o <- liftIO . evaluate . force $ o'
                join <$> safeAtomically (stm o)


    -- | Create a ParConduit from a Conduit.
    --
    -- This is the preferred way to create a ParConduit.
    liftConduit :: forall i o m r .
                    (MonadUnliftIO m
                    , NFData o)
                    => ConduitT i o m r
                    -> ParConduit m r i o
    liftConduit cond = basicParConduit go
        where
            go :: m (Maybe i)
                  -> (o -> m (Maybe ()))
                  -> m r
            go rd wr = connect (src rd) (fuseUpstream cond (sink wr))

            src :: m (Maybe i) -> ConduitT () i m ()
            src rd = do
                mi <- lift rd
                case mi of
                    Nothing -> pure ()
                    Just i -> do
                        yield i
                        src rd

            sink :: (o -> m (Maybe ())) -> ConduitT o Void m ()
            sink wr = do
                mo <- await
                case mo of
                    Nothing -> pure ()
                    Just o -> do
                        mu :: Maybe () <- lift $ wr o
                        case mu of
                            Nothing -> pure ()
                            Just () -> sink wr
    
