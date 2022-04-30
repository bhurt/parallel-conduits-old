{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Conduit.Cache
-- Description : Parallel Conduit Cache
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
-- Provide the cache function.
module Data.Conduit.Parallel.Internal.Conduit.Cache where

    import           Control.Monad.IO.Unlift
    import           GHC.Natural             (Natural)

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Spawn
 
    cache :: forall m a .
                MonadUnliftIO m
                => Natural
                -> ParConduit m () a a
    cache size = ParConduit go
        where
            go :: ReadDuct m a
                    -> WriteDuct m a
                    -> ControlThread m (m ())
            go rd wd = do
                q :: Duct m a <- createBoundedQueueDuct size
                mu1 :: m () <- spawn $ copyThread rd (getWriteEndpoint q)
                mu2 :: m () <- spawn $ copyThread (getReadEndpoint q) wd
                pure $ mu1 >> mu2


