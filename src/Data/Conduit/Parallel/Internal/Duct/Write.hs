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

    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Duct


    data ClosedDuctException = ClosedDuctException
        deriving (Show)

    instance Exception ClosedDuctException

    writeBoth :: forall a b .
                    (a -> STM IsOpen)
                    -> (b -> STM IsOpen)
                    -> (a, b)
                    -> STM IsOpen
    writeBoth wa wb (a, b) = catchSTM foo onE
        where

            -- Note: we throw (with throwSTM) an exception when we
            -- hit a closed write duct.  This undoes all the writes
            -- we did to all the ducts up until that point.  Which
            -- is the behavior we want- we either write to all the
            -- write ducts, or none of them.  Note that this also
            -- works when one (or more) ducts are full and can not
            -- be written to- we retry everything.
            foo :: STM IsOpen
            foo = do
                go a wa
                go b wb
                pure IsOpen

            
            go :: forall x . x -> (x -> STM IsOpen) -> STM ()
            go x wx = do
                r <- wx x
                case r of
                    IsOpen   -> pure ()
                    IsClosed -> throwSTM ClosedDuctException

            onE :: ClosedDuctException -> STM IsOpen
            onE ClosedDuctException = pure IsClosed

    testAndSetIsOpen :: TVar IsOpen
                        -> STM IsOpen
                        -> STM IsOpen
    testAndSetIsOpen tvar act = do
        s <- readTVar tvar
        case s of
            IsClosed -> pure IsClosed
            IsOpen   -> do
                r <- act
                case r of
                    IsOpen   -> pure IsOpen
                    IsClosed -> do
                        writeTVar tvar IsClosed
                        pure IsClosed

    catchRetry :: forall a .
                    STM a
                    -> STM (Maybe a)
    catchRetry act = (Just <$> act) `orElse` pure Nothing

    writeTuple :: forall m a b .
                    MonadIO m
                    => WriteDuct m a
                    -> WriteDuct m b
                    -> WriteDuct m (a,b)
    writeTuple wda wdb = WriteDuct go
        where
            go :: WorkerThread m ((a, b) -> STM IsOpen)
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                tvar <- newTVarIO IsOpen
                pure $ \ (a,b) -> testAndSetIsOpen tvar (writeBoth wa wb (a, b))

    writeEither :: forall a b m .
                        MonadIO m
                        => WriteDuct m a
                        -> WriteDuct m b
                        -> WriteDuct m (Either a b)
    writeEither wda wdb = WriteDuct go
        where
            go :: WorkerThread m (Either a b -> STM IsOpen)
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                tvar <- newTVarIO IsOpen
                pure $ doWrite tvar wa wb

            doWrite :: TVar IsOpen
                        -> (a -> STM IsOpen)
                        -> (b -> STM IsOpen)
                        -> Either a b
                        -> STM IsOpen
            doWrite tvar wa _  (Left a)  = testAndSetIsOpen tvar (wa a)
            doWrite tvar _  wb (Right b) = testAndSetIsOpen tvar (wb b)

    writeThese :: forall a b m .
                        MonadIO m
                        => WriteDuct m a
                        -> WriteDuct m b
                        -> WriteDuct m (These a b)
    writeThese wda wdb = WriteDuct go
        where
            go :: WorkerThread m (These a b -> STM IsOpen)
            go = do
                wa <- getWriteDuct wda
                wb <- getWriteDuct wdb
                tvar <- newTVarIO IsOpen
                pure $ doWrite tvar wa wb

            doWrite :: TVar IsOpen
                        -> (a -> STM IsOpen)
                        -> (b -> STM IsOpen)
                        -> These a b
                        -> STM IsOpen
            doWrite tvar wa _  (This  a  ) = testAndSetIsOpen tvar (wa a)
            doWrite tvar _  wb (That    b) = testAndSetIsOpen tvar (wb b)
            doWrite tvar wa wb (These a b) =
                testAndSetIsOpen tvar (writeBoth wa wb (a, b))

    writeAll :: forall a m . MonadIO m => [ WriteDuct m a ] -> WriteDuct m a
    writeAll ducts = WriteDuct go
        where
            go :: WorkerThread m (a -> STM IsOpen)
            go = do
                writes :: [ (a -> STM IsOpen) ] <- mapM getWriteDuct ducts
                tvar <- newTVarIO IsOpen
                pure $ doWrite tvar writes

            doWrite :: TVar IsOpen -> [ (a -> STM IsOpen) ] -> a -> STM IsOpen
            doWrite tvar ws a = do
                -- Same stunt with throwSTM that we did with writeTuple.
                testAndSetIsOpen tvar (catchSTM (loop ws a) onE)

            onE :: ClosedDuctException  -> STM IsOpen
            onE ClosedDuctException = pure IsClosed

            loop :: [ (a -> STM IsOpen) ] -> a -> STM IsOpen
            loop ws a = do
                mapM_ (doAWrite a) ws
                pure IsOpen

            doAWrite :: a -> (a -> STM IsOpen) -> STM ()
            doAWrite a wa = do
                r <- wa a
                case r of
                    IsOpen   -> pure ()
                    IsClosed -> throwSTM ClosedDuctException

    writeAny :: forall a m . MonadIO m => [ WriteDuct m a ] -> WriteDuct m a
    writeAny ducts = WriteDuct go
        where
            go :: WorkerThread m (a -> STM IsOpen)
            go = do
                writes :: [ (a -> STM IsOpen) ] <- mapM getWriteDuct ducts
                wvar <- newTVarIO . Seq.fromList $ writes
                pure $ doWrite wvar

            doWrite :: TVar (Seq (a -> STM IsOpen))
                        -> a
                        -> STM IsOpen
            doWrite wvar a = do
                s1 :: Seq (a -> STM IsOpen) <- readTVar wvar
                (open, s2) <- writeLoop s1 Seq.empty a
                writeTVar wvar s2
                pure open

            writeLoop :: Seq (a -> STM IsOpen)
                            -> Seq (a -> STM IsOpen)
                            -> a
                            -> STM (IsOpen, Seq (a -> STM IsOpen))
            writeLoop ds fulls a = do
                case Seq.viewl ds of
                    Seq.EmptyL
                        | Seq.null fulls -> pure (IsClosed, Seq.empty)
                        | otherwise      -> retry
                    w Seq.:< ws          -> do
                        r <- catchRetry (w a)
                        case r of
                            Nothing ->
                                -- This specific duct is full.
                                writeLoop ws (fulls Seq.|> w) a
                            Just IsOpen ->
                                -- Write succeeded
                                pure (IsOpen, ws Seq.>< (fulls Seq.|> w))
                            Just IsClosed ->
                                -- Duct is closed- drop it from the list.
                                writeLoop ws fulls a

    
    writeSeq :: forall a m .  MonadIO m => [ WriteDuct m a ] -> WriteDuct m a
    writeSeq ducts = WriteDuct go
        where
            go :: WorkerThread m (a -> STM IsOpen)
            go = do
                writes :: [ (a -> STM IsOpen) ] <- mapM getWriteDuct ducts
                wvar <- newTVarIO . Seq.fromList $ writes
                pure $ doWrite wvar
    
            doWrite :: TVar (Seq (a -> STM IsOpen))
                        -> a
                        -> STM IsOpen
            doWrite wvar a = do
                ws1 :: Seq (a -> STM IsOpen) <- readTVar wvar
                case Seq.viewl ws1 of
                    Seq.EmptyL     -> pure IsClosed
                    (w Seq.:< ws2) -> do
                        r <- w a
                        case r of
                            IsOpen -> do
                                writeTVar wvar (ws2 Seq.|> w)
                                pure IsOpen
                            IsClosed-> do
                                -- Drop the closed duct from our list
                                writeTVar wvar ws2
                                doWrite wvar a

