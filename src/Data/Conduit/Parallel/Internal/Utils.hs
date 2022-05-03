{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Utils
-- Description : Common Functions between Parallel Conduits and Arrows.
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
-- Factor out some common code between Parallel Conduits and Parallel
-- Arrows.

module Data.Conduit.Parallel.Internal.Utils where

    import           Control.Monad.IO.Unlift
    import           Control.Selective
    import           Data.These

    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Spawn

    fuseInternal :: forall m r1 r2 r i x o .
                    MonadUnliftIO m
                    => (r1 -> r2 -> r)
                    -- ^ Combine results (mappend)
                    -> (ReadDuct m i
                            -> WriteDuct m x
                            -> ControlThread m (m r1))
                    -- ^ @ParConduit m r1 i x@ or @ParArrow m i x@
                    -> (ReadDuct m x
                            -> WriteDuct m o
                            -> ControlThread m (m r2))
                    -- ^ @ParConduit m r2 x o@ or @ParArrow m x o@
                    -> (ReadDuct m i
                        -> WriteDuct m o
                        -> ControlThread m (m r))
                    -- ^ @ParConduit m r i o@ or @ParArrow m i o@
    fuseInternal f pc1 pc2 ri wo = do
        d <- createSimpleDuct
        m1 <- pc1 ri (getWriteEndpoint d)
        m2 <- pc2 (getReadEndpoint d) wo
        pure $ f <$> m1 <*> m2


    splice :: forall i1 r1 o1 i2 r2 o2 i r o m .
                MonadUnliftIO m
                => (r1 -> r2 -> r)
                -- ^ Combine results (mappend)
                -> (WriteDuct m i1 -> WriteDuct m i2
                        -> WriteDuct m i)
                -- ^ Combine inputs
                -> (ReadDuct m o1 -> ReadDuct m o2
                        -> ReadDuct m o)
                -- ^ Combine outputs
                -> (ReadDuct m i1
                    -> WriteDuct m o1
                    -> ControlThread m (m r1))
                -- ^ @ParConduit m r1 i1 o1@ or @ParArrow m i1 o1@
                -> (ReadDuct m i2
                    -> WriteDuct m o2
                    -> ControlThread m (m r2))
                -- ^ @ParConduit m r2 i2 o2@ or @ParArrow m i2 o2@ 
                -> (ReadDuct m i
                    -> WriteDuct m o
                    -> ControlThread m (m r))
                -- ^ @ParConduit m r i o@ or @ParArrow m i o@
    splice fixR mergeWrite mergeRead pc1 pc2 rd wr = do
        di1 :: Duct m i1 <- createSimpleDuct
        di2 :: Duct m i2 <- createSimpleDuct
        do1 :: Duct m o1 <- createSimpleDuct
        do2 :: Duct m o2 <- createSimpleDuct
        m1 :: m r1 <- pc1 (getReadEndpoint di1)
                            (getWriteEndpoint do1)
        m2 :: m r2 <- pc2 (getReadEndpoint di2)
                            (getWriteEndpoint do2)
        m3 :: m () <- spawn $ copyThread rd
                                (mergeWrite
                                    (getWriteEndpoint di1)
                                    (getWriteEndpoint di2))
        m4 :: m () <- spawn $ copyThread
                                (mergeRead
                                    (getReadEndpoint do1)
                                    (getReadEndpoint do2))
                                wr
        let r :: m r
            r = do
                    r1 <- m1
                    r2 <- m2
                    m3
                    m4
                    pure $ fixR r1 r2
        pure $ r


    bypass :: forall m i o i1 o1 r .
                MonadUnliftIO m
                => (i -> Either o (i1, o1 -> o))
                -> (ReadDuct m i1
                    -> WriteDuct m o1
                    -> ControlThread m (m r))
                -> ReadDuct m i
                -> WriteDuct m o
                -> ControlThread m (m r)
    bypass foo inner rdi wdo = do
            i1duct :: Duct m i1 <- createSimpleDuct
            o1duct :: Duct m o1 <- createSimpleDuct
            tduct :: Duct m (Either (o1 -> o) o) <- createUnboundedQueueDuct
            mr :: m r <- inner (getReadEndpoint i1duct)
                                    (getWriteEndpoint o1duct)
            let wdi :: WriteDuct m i
                wdi = writeThose bar (getWriteEndpoint tduct)
                                            (getWriteEndpoint i1duct)

                rdo :: ReadDuct m o
                rdo = select (getReadEndpoint tduct)
                            (baz <$> getReadEndpoint o1duct)

            mu1 :: m () <- spawn $ copyThread rdi wdi
            mu2 :: m () <- spawn $ copyThread rdo wdo
            pure $ mu1 >> mu2 >> mr

        where
            bar :: i -> These (Either (o1 -> o) o) i1
            bar i = case foo i of
                        Left o          -> This $ Right o
                        Right (i1, o1o) -> These (Left o1o) i1

            baz :: o1 -> (o1 -> o) -> o
            baz = flip ($)


