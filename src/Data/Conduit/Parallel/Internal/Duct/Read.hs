{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct.Read
-- Description : Parallel Conduit Ducts
-- Copyright   : (c) Brian Hurt, 2022
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything -- in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.

module Data.Conduit.Parallel.Internal.Duct.Read(
    readTuple,
    readMerge
) where

    import           Control.Monad.STM
    -- import           Data.Sequence     (Seq)
    -- import qualified Data.Sequence     as Seq
    -- import           Data.These        (These (..))
    import           UnliftIO

    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Utils
    import           Data.Conduit.Parallel.Internal.Spawn


    readBoth :: forall a b .
                    STM (Maybe a)
                    -> STM (Maybe b)
                    -> STM (Maybe (a, b))
    readBoth ra rb = catchSTM foo onE
        where
            foo :: STM (Maybe (a, b))
            foo = do
                a <- bar ra
                b <- bar rb
                pure $ Just (a, b)

            bar :: forall x .  STM (Maybe x) -> STM x
            bar rx = do
                r <- rx
                case r of
                    Just x -> pure x
                    Nothing -> throwSTM ClosedDuctException

            onE :: ClosedDuctException -> STM (Maybe (a, b))
            onE ClosedDuctException = pure Nothing

    readTuple :: forall a b m .
                    MonadIO m
                    => ReadDuct m a
                    -> ReadDuct m b
                    -> ReadDuct m (a, b)
    readTuple rda rdb = ReadDuct go
        where
            go :: WorkerThread m (STM (Maybe (a, b)))
            go = do
                ra :: STM (Maybe a) <- getReadDuct rda
                rb :: STM (Maybe b) <- getReadDuct rdb
                checkClosed ($ readBoth ra rb)

