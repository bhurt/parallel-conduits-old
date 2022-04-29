{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Conduit.Portal
-- Description : Definition of the portal function.
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
module Data.Conduit.Parallel.Internal.Conduit.Portal where

    import           Control.Monad.IO.Unlift
    import           Data.Void               (Void)

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Spawn

    portal :: forall m r a i o .
                MonadUnliftIO m
                => (ParConduit m () () a
                    -> ParConduit m () a Void
                    -> ParConduit m r i o)
                -> ParConduit m r i o
    portal f = ParConduit go
        where
            go :: ReadDuct m i
                    -> WriteDuct m o
                    -> ControlThread m (m r)
            go rdi wdo = do
                duct :: Duct m a <- createSimpleDuct
                let res :: ParConduit m r i o
                    res = f (source (getReadEndpoint duct))
                            (sink (getWriteEndpoint duct))
                getParConduit res rdi wdo

            source :: ReadDuct m a -> ParConduit m () () a
            source rda = ParConduit go'
                where
                    go' :: ReadDuct m ()
                            -> WriteDuct m a
                            -> ControlThread m (m ())
                    go' _ wda = spawn $ copyThread rda wda

            sink :: WriteDuct m a -> ParConduit m () a Void
            sink wda = ParConduit go'
                where
                    go' :: ReadDuct m a
                            -> WriteDuct m Void
                            -> ControlThread m (m ())
                    go' rda _ = spawn $ copyThread rda wda


