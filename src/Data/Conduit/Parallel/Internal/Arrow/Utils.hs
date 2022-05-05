{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Arrow.Utils
-- Description : Arrow-specific Utility Functions
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
-- Where to collect Arrow-specific utility functions.
--
module Data.Conduit.Parallel.Internal.Arrow.Utils where

    import           Control.Concurrent.STM
    import           Control.Monad.IO.Unlift
    import           Control.Selective
    import           Data.These

    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Duct.Utils
    import           Data.Conduit.Parallel.Internal.Spawn

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

    dispatch :: forall m i o i1 o1 i2 o2 r .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => (i -> Either (i1, o1 -> o) (i2, o2 -> o))
                    -> (ReadDuct m i1
                        -> WriteDuct m o1
                        -> ControlThread m (m r))
                    -> (ReadDuct m i2
                        -> WriteDuct m o2
                        -> ControlThread m (m r))
                    -> ReadDuct m i
                    -> WriteDuct m o
                    -> ControlThread m (m r)
    dispatch dir inner1 inner2 rdi wdo = do
            i1duct :: Duct m i1 <- createSimpleDuct
            o1duct :: Duct m o1 <- createSimpleDuct
            i2duct :: Duct m i2 <- createSimpleDuct
            o2duct :: Duct m o2 <- createSimpleDuct
            tduct :: Duct m (Either (o1 -> o) (o2 -> o))
                <- createUnboundedQueueDuct
            mr1 :: m r <- inner1 (getReadEndpoint i1duct)
                                    (getWriteEndpoint o1duct)
            mr2 :: m r <- inner2 (getReadEndpoint i2duct)
                                    (getWriteEndpoint o2duct)
            let mr :: m r
                mr = (<>) <$> mr1 <*> mr2

                wdi :: WriteDuct m i
                wdi = mergeWrites (getWriteEndpoint tduct)
                        (getWriteEndpoint i1duct)
                        (getWriteEndpoint i2duct)

                rdo :: ReadDuct m o
                rdo= mergeReads (getReadEndpoint tduct)
                        (getReadEndpoint o1duct)
                        (getReadEndpoint o2duct)

            mu1 :: m () <- spawn $ copyThread rdi wdi
            mu2 :: m () <- spawn $ copyThread rdo wdo
            pure $ mu1 >> mu2 >> mr

        where
            mergeWrites :: WriteDuct m (Either (o1 -> o) (o2 -> o))
                            -> WriteDuct m i1
                            -> WriteDuct m i2
                            -> WriteDuct m i
            mergeWrites wdt wdi1 wdi2 = WriteDuct go
                where
                    go :: WorkerThread m (i -> STM (Maybe ()))
                    go = do
                        wt <- getWriteDuct wdt
                        wi1 <- getWriteDuct wdi1
                        wi2 <- getWriteDuct wdi2
                        pure $ doWrite wt wi1 wi2

                    doWrite :: (Either (o1 -> o) (o2 -> o) -> STM (Maybe ()))
                                -> (i1 -> STM (Maybe ()))
                                -> (i2 -> STM (Maybe ()))
                                -> i -> STM (Maybe ())
                    doWrite wt wi1 wi2 i = catchClosedDuct $ do
                        case dir i of
                            Left (i1, o1) -> do
                                throwClosed $ wi1 i1
                                throwClosed $ wt (Left o1)
                                pure $ Just ()
                            Right (i2, o2) -> do
                                throwClosed $ wi2 i2
                                throwClosed $ wt (Right o2)
                                pure $ Just ()

                

            mergeReads :: ReadDuct m (Either (o1 -> o) (o2 -> o))
                            -> ReadDuct m o1
                            -> ReadDuct m o2
                            -> ReadDuct m o
            mergeReads rdt rdo1 rdo2 = ReadDuct go
                where
                    go :: WorkerThread m (STM (Maybe o))
                    go = do
                        rt <- getReadDuct rdt
                        ro1 <- getReadDuct rdo1
                        ro2 <- getReadDuct rdo2
                        pure $ doRead rt ro1 ro2

                    doRead :: STM (Maybe (Either (o1 -> o) (o2 -> o)))
                                -> STM (Maybe o1)
                                -> STM (Maybe o2)
                                -> STM (Maybe o)
                    doRead rt ro1 ro2 = do
                        t <- rt
                        case t of
                            Nothing -> pure Nothing
                            Just (Left  o1) -> fmap o1 <$> ro1
                            Just (Right o2) -> fmap o2 <$> ro2




