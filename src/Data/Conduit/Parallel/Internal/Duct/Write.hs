{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct.Write
-- Description : Write Duct Combinators
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
-- This module contains various functions for combining several
-- write endpoints into a single endpoint.
-- 
-- = Justification
--
-- The nice thing about using STM is that it /composes/.  It's natural
-- to combine multiple STM actions into a single action.  So we can
-- move most of the routing logic into STM actions.  So this module
-- and [Duct.Read](Data-Conduit-Parallel-Internal-Duct-Read.html)
-- end up with most of the logic of the whole library.
--
module Data.Conduit.Parallel.Internal.Duct.Write(
    writeAll,
    writeAny,
    writeSeq
) where

    import           Control.Monad.STM
    import           Data.Sequence              (Seq)
    import qualified Data.Sequence              as Seq
    import           UnliftIO

    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Utils
    import           Data.Conduit.Parallel.Internal.Spawn

    isClosed :: forall a m .
                    MonadIO m
                    => (a -> STM (Maybe ()))
                    -> m (a -> STM (Maybe ()))
    isClosed act = enforceClosed (\f -> (\a -> f (act a)))

    writeAll :: forall a m .
                    MonadIO m
                    => [ WriteDuct m a ]
                    -> WriteDuct m a
    writeAll ducts = WriteDuct go
        where
            go :: WorkerThread m (a -> STM (Maybe ()))
            go = do
                writes :: [ (a -> STM (Maybe ())) ] <- mapM getWriteDuct ducts
                isClosed $ doWrite writes

            doWrite :: [ (a -> STM (Maybe ())) ] -> a -> STM (Maybe ())
            doWrite ws a = catchClosedDuct $ do
                                mapM_ (\d -> throwClosed (d a)) ws
                                pure $ Just ()

    writeAny :: forall a m .
                    MonadIO m
                    => [ WriteDuct m a ]
                    -> WriteDuct m a
    writeAny ducts = WriteDuct go
        where
            go :: WorkerThread m (a -> STM (Maybe ()))
            go = do
                writes :: [ (a -> STM (Maybe ())) ] <- mapM getWriteDuct ducts
                wvar <- newTVarIO . Seq.fromList $ writes
                isClosed $ doWrite wvar

            doWrite :: TVar (Seq (a -> STM (Maybe ())))
                        -> a
                        -> STM (Maybe ())
            doWrite wvar a = do
                s1 :: Seq (a -> STM (Maybe ())) <- readTVar wvar
                (open, s2) <- writeLoop s1 Seq.empty a
                writeTVar wvar s2
                pure open

            writeLoop :: Seq (a -> STM (Maybe ()))
                            -> Seq (a -> STM (Maybe ()))
                            -> a
                            -> STM ((Maybe ()), Seq (a -> STM (Maybe ())))
            writeLoop ds fulls a = do
                case Seq.viewl ds of
                    Seq.EmptyL
                        | Seq.null fulls -> pure (Nothing, Seq.empty)
                        | otherwise      -> retry
                    w Seq.:< ws          -> do
                        r <- catchRetry (w a)
                        case r of
                            Nothing ->
                                -- This specific duct is full.
                                writeLoop ws (fulls Seq.|> w) a
                            Just (Just ()) ->
                                -- Write succeeded
                                pure (Just (), ws Seq.>< (fulls Seq.|> w))
                            Just Nothing ->
                                -- Duct is closed- drop it from the list.
                                writeLoop ws fulls a

    
    writeSeq :: forall a m . 
                    MonadIO m
                    => [ WriteDuct m a ]
                    -> WriteDuct m a
    writeSeq ducts = WriteDuct go
        where
            go :: WorkerThread m (a -> STM (Maybe ()))
            go = do
                writes :: [ (a -> STM (Maybe ())) ] <- mapM getWriteDuct ducts
                wvar <- newTVarIO . Seq.fromList $ writes
                isClosed $ doWrite wvar
    
            doWrite :: TVar (Seq (a -> STM (Maybe ())))
                        -> a
                        -> STM (Maybe ())
            doWrite wvar a = do
                ws1 :: Seq (a -> STM (Maybe ())) <- readTVar wvar
                case Seq.viewl ws1 of
                    Seq.EmptyL     -> pure Nothing
                    (w Seq.:< ws2) -> do
                        r <- w a
                        case r of
                            Just () -> do
                                writeTVar wvar (ws2 Seq.|> w)
                                pure (Just ())
                            Nothing -> do
                                -- Drop the closed duct from our list
                                writeTVar wvar ws2
                                doWrite wvar a

