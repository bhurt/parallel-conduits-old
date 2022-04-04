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

module Data.Conduit.Parallel.Internal.Duct.Read where

    import           Control.Concurrent.STM
    import           Control.Exception
    import           Data.Functor.Contravariant
    import           Data.Sequence              (Seq)
    import qualified Data.Sequence              as Seq
    import           Data.These                 (These (..))
    import           Data.Void

    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Duct


    readTuple :: forall a b .
                    ReadDuct a
                    -> ReadDuct b
                    -> ReadDuct (a, b)
    readTuple rda rdb = ReadDuct {
                            readDuctSTM = doRead,
                            closeReadDuctSTM = doClose }
        where
            doRead :: STM (Maybe (a, b))
            doRead = do
                ma :: Maybe a <- readDuctSTM rda
                case ma of
                    Nothing -> do
                        closeReadDuctSTM rdb
                        pure Nothing
                    Just a -> do
                        mb :: Maybe b <- readDuctSTM rdb
                        case mb of
                            Nothing -> do
                                -- The fact that we have already read an
                                -- element from the a duct doesn't matter
                                -- here- we're discarding it anyways.
                                closeReadDuctSTM rda
                                pure Nothing
                            Just b -> pure $ Just (a, b)

            doClose :: STM ()
            doClose = do
                closeReadDuctSTM rda
                closeReadDuctSTM rdb
                pure ()



