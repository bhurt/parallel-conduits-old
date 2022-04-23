{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Conduit.Route
-- Description : Routing Functions for Parallel Conduits
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
-- Routing functions for Parallel Conduits.
module Data.Conduit.Parallel.Internal.Conduit.Route where

    import           Control.Monad.IO.Unlift
    import           Data.Functor.Contravariant.Divisible
    import           Data.Profunctor
    import           Data.These
    import           Data.Void

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Duct.No
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Utils

    splitThese :: forall m r a b c .
                (MonadUnliftIO m
                , Semigroup r)
                => ParConduit m r a c
                -> ParConduit m r b c
                -> ParConduit m r (These a b) c
    splitThese ac bc = ParConduit $
                            splice (<>) (writeThose id) alternate
                                (getParConduit ac) (getParConduit bc)

    splitTuple :: forall m a b c .
                MonadUnliftIO m
                => ParConduit m () a c
                -> ParConduit m () b c
                -> ParConduit m () (a, b) c
    splitTuple ac bc = ParConduit $
                            splice (<>) (divide id) alternate
                                (getParConduit ac) (getParConduit bc)

    split :: forall m a b c .
                MonadUnliftIO m
                => ParConduit m () a c
                -> ParConduit m () b c
                -> ParConduit m () (Either a b) c
    split ac bc = ParConduit $
                            splice (<>) (choose id) alternate
                                (getParConduit ac) (getParConduit bc)


    merge :: forall m r a .
                MonadUnliftIO m
                => ParConduit m r () a
                -> ParConduit m r a  a
    merge inner = ParConduit $ go
        where
            go :: ReadDuct m a
                    -> WriteDuct m a
                    -> ControlThread m (m r)
            go rd wd = do
                d :: Duct m a <- createSimpleDuct
                mr :: m r <- getParConduit inner noRead (getWriteEndpoint d)
                let rd1 :: ReadDuct m a = alternate rd (getReadEndpoint d)
                mu :: m () <- spawn $ copyThread rd1 wd
                pure $ mu >> mr

    mergeEither :: forall m r a b .
                    MonadUnliftIO m
                    => ParConduit m r () b
                    -> ParConduit m r a (Either b a)
    mergeEither inner = lmap Right $ merge (Left <$> inner)

    routeShared :: forall m r a b c .
                    MonadUnliftIO m
                    => (a -> These b c)
                    -> ParConduit m r b Void
                    -> ParConduit m r a c
    routeShared f inner = ParConduit $ go
        where
            go :: ReadDuct m a
                    -> WriteDuct m c
                    -> ControlThread m (m r)
            go rda wdc = do
                d :: Duct m b <- createSimpleDuct
                mr :: m r <- getParConduit inner (getReadEndpoint d) noWrite
                let wda = writeThose f (getWriteEndpoint d) wdc
                mu :: m () <- spawn $ copyThread rda wda
                pure $ mu >> mr

    routeThese :: forall m r a b .
                MonadUnliftIO m
                => ParConduit m r b Void
                -> ParConduit m r (These b a) a
    routeThese = routeShared id

    routeTuple :: forall m r a b .
                MonadUnliftIO m
                => ParConduit m r b Void
                -> ParConduit m r (b, a) a
    routeTuple = routeShared (\(b, a) -> These b a)

    route :: forall m r a b .
                MonadUnliftIO m
                => ParConduit m r b Void
                -> ParConduit m r (Either b a) a
    route = routeShared f
        where
            f :: Either b a -> These b a
            f (Left b)  = This b
            f (Right a) = That a

