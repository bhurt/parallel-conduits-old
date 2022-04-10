{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct.Read
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

module Data.Conduit.Parallel.Internal.Duct.Read(
    readBoth,
    readTuple
) where

    import           Control.Monad.STM
    -- import           Data.Sequence     (Seq)
    -- import qualified Data.Sequence     as Seq
    -- import           Data.These        (These (..))
    import           UnliftIO

    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Utils
    import           Data.Conduit.Parallel.Internal.Spawn


    readBoth :: forall m a b c .
                    MonadIO m
                    => (a -> b -> c)
                    -> ReadDuct Simple m a
                    -> ReadDuct Simple m b
                    -> ReadDuct Complex m c
    readBoth f rda rdb = ReadDuct go
        where
            go :: WorkerThread m (STM (Maybe c))
            go = do
                ra :: STM (Maybe a) <- getReadDuct rda
                rb :: STM (Maybe b) <- getReadDuct rdb
                pure $ doRead ra rb

            doRead :: STM (Maybe a)
                        -> STM (Maybe b)
                        -> STM (Maybe c)
            doRead ra rb = catchClosedDuct $ do
                                a <- throwClosed ra
                                b <- throwClosed rb
                                pure $ f a b

    readTuple :: forall a b m .
                    MonadIO m
                    => ReadDuct Simple m a
                    -> ReadDuct Simple m b
                    -> ReadDuct Complex m (a, b)
    readTuple = readBoth (,)

