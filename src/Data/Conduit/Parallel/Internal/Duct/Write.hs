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
-- instead.  Anything -- in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Duct.Write(
    writeTuple,
    writeEither,
    writeThese,
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

    writeBoth :: forall a b .
                    (a -> STM (Maybe ()))
                    -> (b -> STM (Maybe ()))
                    -> (a, b)
                    -> STM (Maybe ())
    writeBoth wa wb (a, b) = catchSTM foo onE
        where

            -- Note: we throw (with throwSTM) an exception when we
            -- hit a closed write duct.  This undoes all the writes
            -- we did to all the ducts up until that point.  Which
            -- is the behavior we want- we either write to all the
            -- write ducts, or none of them.  Note that this also
            -- works when one (or more) ducts are full and can not
            -- be written to- we retry everything.
            foo :: STM (Maybe ())
            foo = do
                go a wa
                go b wb
                pure (Just ())

            
            go :: forall x . x -> (x -> STM (Maybe ())) -> STM ()
            go x wx = do
                r <- wx x
                case r of
                    Just () -> pure ()
                    Nothing -> throwSTM ClosedDuctException

            onE :: ClosedDuctException -> STM (Maybe ())
            onE ClosedDuctException = pure Nothing

    writeTuple :: forall m a b .
                    WriteDuct Simple m a
                    -> WriteDuct Simple m b
                    -> WriteDuct Complex m (a,b)
    writeTuple wda wdb = WriteDuct go
        where
            go :: WorkerThread m ((a, b) -> STM (Maybe ()))
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                pure $ writeBoth wa wb

    writeEither :: forall a b m .
                        WriteDuct Simple m a
                        -> WriteDuct Simple m b
                        -> WriteDuct Complex m (Either a b)
    writeEither wda wdb = WriteDuct go
        where
            go :: WorkerThread m (Either a b -> STM (Maybe ()))
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                pure $ doWrite wa wb

            doWrite :: (a -> STM (Maybe ()))
                        -> (b -> STM (Maybe ()))
                        -> Either a b
                        -> STM (Maybe ())
            doWrite wa _  (Left a)  = wa a
            doWrite _  wb (Right b) = wb b

    writeThese :: forall a b m .
                        WriteDuct Simple m a
                        -> WriteDuct Simple m b
                        -> WriteDuct Complex m (These a b)
    writeThese wda wdb = WriteDuct go
        where
            go :: WorkerThread m (These a b -> STM (Maybe ()))
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                pure $ doWrite wa wb

            doWrite :: (a -> STM (Maybe ()))
                        -> (b -> STM (Maybe ()))
                        -> These a b
                        -> STM (Maybe ())
            doWrite wa _  (This  a  ) = wa a
            doWrite _  wb (That    b) = wb b
            doWrite wa wb (These a b) = writeBoth wa wb (a, b)

    writeAll :: forall a m .
                    [ WriteDuct Simple m a ]
                    -> WriteDuct Complex m a
    writeAll ducts = WriteDuct go
        where
            go :: WorkerThread m (a -> STM (Maybe ()))
            go = do
                writes :: [ (a -> STM (Maybe ())) ] <- mapM getWriteDuct ducts
                pure $ doWrite writes

            -- Same stunt with throwSTM that we did with writeTuple.
            doWrite :: [ (a -> STM (Maybe ())) ] -> a -> STM (Maybe ())
            doWrite ws a = catchSTM (loop ws a) onE

            onE :: ClosedDuctException  -> STM (Maybe ())
            onE ClosedDuctException = pure Nothing

            loop :: [ (a -> STM (Maybe ())) ] -> a -> STM (Maybe ())
            loop ws a = do
                mapM_ (doAWrite a) ws
                pure (Just ())

            doAWrite :: a -> (a -> STM (Maybe ())) -> STM ()
            doAWrite a wa = do
                r <- wa a
                case r of
                    Just () -> pure ()
                    Nothing -> throwSTM ClosedDuctException

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
                pure $ doWrite wvar

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
                pure $ doWrite wvar
    
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

