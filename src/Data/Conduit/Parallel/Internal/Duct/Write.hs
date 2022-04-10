{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct.Write
-- Description : Write Duct Combinators
-- Copyright   : (c) Brian Hurt, 2022
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
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
-- @Simple@ write endpoints into a single @Complex@ endpoint.
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
    writeDuplicate,
    writeTuple,
    writeEither,
    writeThese,
    writeEitherWitness,
    writeTheseWitness,
    writeAll,
    writeAny,
    writeSeq
) where

    import           Control.Monad.STM
    import           Data.Sequence     (Seq)
    import qualified Data.Sequence     as Seq
    import           Data.These        (These (..))
    import           UnliftIO

    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Utils
    import           Data.Conduit.Parallel.Internal.Spawn

    isClosed :: forall a m .
                    MonadIO m
                    => (a -> STM (Maybe ()))
                    -> m (a -> STM (Maybe ()))
    isClosed act = enforceClosed (\f -> (\a -> f (act a)))


    writeBoth :: forall a b .
                    (a -> STM (Maybe ()))
                    -> (b -> STM (Maybe ()))
                    -> (a, b)
                    -> STM (Maybe ())
    writeBoth wa wb (a, b) = catchClosedDuct $ do
                                throwClosed $ wa a
                                throwClosed $ wb b
                                pure $ Just ()

    writeDuplicate :: forall m a .
                        MonadIO m
                        => WriteDuct Simple m a
                        -> WriteDuct Simple m a
                        -> WriteDuct Simple m a
    writeDuplicate wd1 wd2 = WriteDuct go
        where
            go :: WorkerThread m (a -> STM (Maybe ()))
            go = do
                w1 <- getWriteDuct wd1
                w2 <- getWriteDuct wd2
                isClosed $ doWrite w1 w2

            doWrite :: (a -> STM (Maybe ()))
                        -> (a -> STM (Maybe ()))
                        -> a
                        -> STM (Maybe ())
            doWrite w1 w2 a = catchClosedDuct $ do
                throwClosed $ w1 a
                throwClosed $ w2 a
                pure $ Just ()

    writeTuple :: forall m a b .
                    MonadIO m
                    => WriteDuct Simple m a
                    -> WriteDuct Simple m b
                    -> WriteDuct Complex m (a,b)
    writeTuple wda wdb = WriteDuct go
        where
            go :: WorkerThread m ((a, b) -> STM (Maybe ()))
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                isClosed $ writeBoth wa wb

    writeEither :: forall a b m .
                        MonadIO m
                        => WriteDuct Simple m a
                        -> WriteDuct Simple m b
                        -> WriteDuct Complex m (Either a b)
    writeEither wda wdb = WriteDuct go
        where
            go :: WorkerThread m (Either a b -> STM (Maybe ()))
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                isClosed $ doWrite wa wb

            doWrite :: (a -> STM (Maybe ()))
                        -> (b -> STM (Maybe ()))
                        -> Either a b
                        -> STM (Maybe ())
            doWrite wa _  (Left a)  = wa a
            doWrite _  wb (Right b) = wb b

    writeThese :: forall a b m .
                        MonadIO m
                        => WriteDuct Simple m a
                        -> WriteDuct Simple m b
                        -> WriteDuct Complex m (These a b)
    writeThese wda wdb = WriteDuct go
        where
            go :: WorkerThread m (These a b -> STM (Maybe ()))
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                isClosed $ doWrite wa wb

            doWrite :: (a -> STM (Maybe ()))
                        -> (b -> STM (Maybe ()))
                        -> These a b
                        -> STM (Maybe ())
            doWrite wa _  (This  a  ) = wa a
            doWrite _  wb (That    b) = wb b
            doWrite wa wb (These a b) = writeBoth wa wb (a, b)

    writeEitherWitness :: forall a b m .
                            MonadIO m
                            => WriteDuct Simple m (Either () ())
                            -> WriteDuct Simple m a
                            -> WriteDuct Simple m b
                            -> WriteDuct Complex m (Either a b)
    writeEitherWitness wdw wda wdb = WriteDuct go
        where
            go :: WorkerThread m (Either a b -> STM (Maybe ()))
            go = do
                ww <- getWriteDuct wdw
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                isClosed $ doWrite ww wa wb

            doWrite :: (Either () () -> STM (Maybe ()))
                        -> (a -> STM (Maybe ()))
                        -> (b -> STM (Maybe ()))
                        -> Either a b
                        -> STM (Maybe ())
            doWrite ww wa _  (Left  a) = writeBoth ww wa (Left  (), a)
            doWrite ww _  wb (Right b) = writeBoth ww wb (Right (), b)

    writeTheseWitness :: forall a b m .
                            MonadIO m
                            => WriteDuct Simple m (These () ())
                            -> WriteDuct Simple m a
                            -> WriteDuct Simple m b
                            -> WriteDuct Complex m (These a b)
    writeTheseWitness wdw wda wdb = WriteDuct go
        where
            go :: WorkerThread m (These a b -> STM (Maybe ()))
            go = do
                ww <- getWriteDuct wdw
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                isClosed $ doWrite ww wa wb

            doWrite :: (These () () -> STM (Maybe ()))
                        -> (a -> STM (Maybe ()))
                        -> (b -> STM (Maybe ()))
                        -> These a b
                        -> STM (Maybe ())
            doWrite ww wa _  (This  a  ) = writeBoth ww wa (This  (), a)
            doWrite ww _  wb (That    b) = writeBoth ww wb (That (), b)
            doWrite ww wa wb (These a b) = catchClosedDuct $ do
                throwClosed $ ww (These () ())
                throwClosed $ wa a
                throwClosed $ wb b
                pure $ Just ()

    writeAll :: forall a m .
                    MonadIO m
                    => [ WriteDuct Simple m a ]
                    -> WriteDuct Complex m a
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
                    => [ WriteDuct Simple m a ]
                    -> WriteDuct Complex m a
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
                    => [ WriteDuct Simple m a ]
                    -> WriteDuct Complex m a
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

