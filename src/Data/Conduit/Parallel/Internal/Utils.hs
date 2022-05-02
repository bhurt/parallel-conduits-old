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

    bypass :: forall m i o a b c r .
                MonadUnliftIO m
                => (i -> These a b)
                -> (These a c -> o)
                -> (ReadDuct m b
                    -> WriteDuct m c
                    -> ControlThread m (m r))
                -> ReadDuct m i
                -> WriteDuct m o
                -> ControlThread m (m r)
    bypass pre post inner rdi wdo = do
            bduct :: Duct m b <- createSimpleDuct
            cduct :: Duct m c <- createSimpleDuct
            tduct :: Duct m (These a ()) <- createUnboundedQueueDuct
            mr :: m r <- inner (getReadEndpoint bduct)
                                    (getWriteEndpoint cduct)
            let wdi :: WriteDuct m i
                wdi = writeThose fixedPre (getWriteEndpoint tduct)
                                            (getWriteEndpoint bduct)
                needElem :: ReadDuct m (Either (Maybe a) (These a c))
                needElem = readCache <$> getReadEndpoint tduct

                getElem :: ReadDuct m (Maybe a -> These a c)
                getElem = readElem <$> getReadEndpoint (cduct)

                rdo :: ReadDuct m o
                rdo = post <$> select needElem getElem

            mu1 :: m () <- spawn $ copyThread rdi wdi
            mu2 :: m () <- spawn $ copyThread rdo wdo
            pure $ mu1 >> mu2 >> mr

        where
            fixedPre :: i -> These (These a ()) b
            fixedPre i = case pre i of
                            This a -> This (This a)
                            That b -> These (That ()) b
                            These a b -> These (These a ()) b

            readCache :: These a () -> Either (Maybe a) (These a c)
            readCache (This a) = Right $ This a
            readCache (That ()) = Left Nothing
            readCache (These a ()) = Left $ Just a


            readElem :: c -> Maybe a -> These a c 
            readElem c Nothing  = That c
            readElem c (Just a) = These a c
