{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct.Create
-- Description : Create a simple duct
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
-- This module contains the code to create simple ducts.  Every duct
-- has a queue of some size in it.
--
module Data.Conduit.Parallel.Internal.Duct.Create(
    createSimpleDuct,
    createBoundedQueueDuct,
    createUnboundedQueueDuct
) where

    import qualified Control.Concurrent.STM.TBQueue as TBQueue
    import qualified Control.Concurrent.STM.TQueue  as TQueue
    import           Control.Monad.STM
    import           GHC.Natural                    (Natural)
    import           UnliftIO

    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Duct

    -- | Factored out common code from all the create duct functions.
    createDuct :: forall a m .
                    MonadUnliftIO m
                    => STM (Maybe a)            -- ^ doRead
                    -> STM ()                   -- ^ readClose
                    -> (a -> STM (Maybe ()))    -- ^ doWrite
                    -> STM ()                   -- ^ writeClose
                    -> Duct Simple m a
    createDuct doRead readClose doWrite writeClose = Duct {
                    getReadEndpoint = readEndpoint,
                    getWriteEndpoint = writeEndpoint }
        where
            readEndpoint :: ReadDuct Simple m a
            readEndpoint = ReadDuct $ workerThreadBracket
                                        (pure doRead)
                                        (const (safeAtomically readClose))

            writeEndpoint :: WriteDuct Simple m a
            writeEndpoint = WriteDuct $ workerThreadBracket
                                            (pure doWrite)
                                            (const (safeAtomically writeClose))


    -- | The duct state stored in the TVar
    data DuctState a =
        Empty       -- ^ Duct is empty (reads retry, writes succeed)
        | Full a    -- ^ Duct is full (reads succeed, writes retry)
        | Closed    -- ^ Duct is closed (both reads and writes fail)

    -- | Create a duct with a single element queue.
    --
    -- The commonest form of simple queues, those that can hold
    -- at most a single value.  As every read and write is just
    -- accessing a single TVar, these ducts are @Simple@.
    --
    -- Note that simple ducts are very closely modeled on MVars, just
    -- with the additional ability to be closed.  They can only hold
    -- a single value at a time.
    --
    --  * If the queue holds a value (is @Full@):
    --
    --      * Reads succeed, and the queue becomes empty
    --
    --      * Writes retry
    --
    --  * If the queue does not hold a value (is @Empty@):
    --
    --      * Reads retry
    --
    --      * Writes succeed, and the queue becomes @Full@.
    --
    --  * If the queue is closed, both reads and writes fail.
    --
    createSimpleDuct :: forall a m .
                        MonadUnliftIO m
                        => ControlThread m (Duct Simple m a)
    createSimpleDuct = do
            tvar <- newTVarIO Empty
            return $ createDuct (doRead tvar) (readClose tvar)
                            (doWrite tvar) (writeClose tvar)
        where
            doRead :: TVar (DuctState a) -> STM (Maybe a)
            doRead tvar = do
                            s <- readTVar tvar
                            case s of
                                Empty -> retry
                                Full a -> do
                                    writeTVar tvar Empty
                                    pure $ Just a
                                Closed -> pure Nothing

            readClose :: TVar (DuctState a) -> STM ()
            readClose tvar = writeTVar tvar Closed

            doWrite :: TVar (DuctState a) -> a -> STM (Maybe ())
            doWrite tvar a = do
                s <- readTVar tvar
                case s of
                    Empty -> do
                        writeTVar tvar (Full a)
                        pure (Just ())
                    Full _ -> retry
                    Closed -> pure Nothing

            writeClose :: TVar (DuctState a)
                            -> STM ()
            writeClose tvar = do
                s <- readTVar tvar
                case s of
                    Empty  -> writeTVar tvar Closed
                    Full _ -> retry
                    Closed -> pure ()


    -- | Common code factored out of the create queue duct functions.
    --
    -- We need a TVar to hold the flag that we are closed, plus either
    -- a TBQueue or TQueue (depending) to hold the queue of items.  So
    -- every read or write is at most 2 TVar accesses (one for the flag,
    -- one for the queue).  This is cheap enough for the queues to
    -- still be @Simple@ however.
    --
    createQueueDuct :: forall a m q .
                        MonadUnliftIO m
                        => IO (q a)                
                            -- ^ create the queue (newTQueueIO)
                        -> (q a -> STM (Maybe a))
                            -- ^ try and read from the queue (tryReadTQueue)
                        -> (q a -> a -> STM ())
                            -- ^ write to the queue (writeTQueue)
                        -> (q a -> STM [a])
                            -- ^ flush the queue (flushTQueue)
                        -> ControlThread m (Duct Simple m a)
    createQueueDuct makeQ readQ writeQ flushQ = liftIO $ do
            isOpen <- newTVarIO True
            q <- liftIO makeQ
            return $ createDuct (doRead isOpen q) (readClose isOpen q)
                            (doWrite isOpen q) (writeClose isOpen)
        where
            doRead :: TVar Bool -> q a -> STM (Maybe a)
            doRead isOpen q = do
                r <- readQ q
                case r of
                    Just a  -> pure $ Just a
                    Nothing -> do
                        open <- readTVar isOpen
                        if open
                        then retry
                        else pure Nothing

            readClose :: TVar Bool
                            -> q a
                            -> STM ()
            readClose isOpen q = do
                writeTVar isOpen False
                _ <- flushQ q
                pure ()

            doWrite :: TVar Bool
                            -> q a
                            -> a
                            -> STM (Maybe ())
            doWrite isOpen q a = do
                open <- readTVar isOpen
                if open
                then do
                    writeQ q a
                    pure $ Just ()
                else pure Nothing

            writeClose :: TVar Bool -> STM ()
            writeClose isOpen = writeTVar isOpen False

    -- | Create a duct with a bounded queue.
    --
    -- The queue can contain a given maximum number of elements.  This
    -- is used for creating cache segments.  
    --
    --  * For reads:
    --
    --      * If the queue is closed, the read fails.
    --
    --      * If the queue has more than 0 elements, the least recently
    --          added element is removed from the queue and the read
    --          succeeds (elements are returned in FIFO order).
    --
    --      * If the queue has 0 elements, the read retries.
    --
    --  * For writes:
    --
    --      * If the queue is closed, the write fails.
    --
    --      * If the queue has less than the maximum number of allowed
    --          elements, the write succeeds and the element is added
    --          to the end of the queue.
    --
    --      * If the queue has the maximum number of allowed elements,
    --          the write retries.
    --
    -- Note that when the read close happens, the elements in the queue
    -- are dropped.  But if the write close happens, the elements in
    -- the queue are retained, and the queue isn't closed until all the
    -- elements have been read.  Writes will still fail in this "twilight
    -- time" however.
    --
    createBoundedQueueDuct :: forall a m .
                                MonadUnliftIO m
                                => Natural
                                -> ControlThread m (Duct Simple m a)
    createBoundedQueueDuct n = 
        createQueueDuct (TBQueue.newTBQueueIO n) TBQueue.tryReadTBQueue
                            TBQueue.writeTBQueue TBQueue.flushTBQueue

    -- | Create a duct with an unbounded queue.
    --
    -- The queue can contain any number of elements.  It is assumed
    -- that some other constraint in the system will prevent truely
    -- unbounded growth of the queue, but that determining what that
    -- bound actually is, is difficult.
    --
    -- This is mainly used in the Arrow code to create alternate
    -- channels for values to flow through.  The actual bound is
    -- the capacitance of the other channel.
    --
    --  * For reads:
    --
    --      * If the queue is closed, the read fails.
    --
    --      * If the queue has more than 0 elements, the least recently
    --          added element is removed from the queue and the read
    --          succeeds (elements are returned in FIFO order).
    --
    --      * If the queue has 0 elements, the read retries.
    --
    --  * For writes:
    --
    --      * If the queue is closed, the write fails.
    --
    --      * Otherwise, the write succeeds and the element is added
    --          to the end of the queue.
    --
    --      * Writes never retry.
    --
    -- Note that when the read close happens, the elements in the queue
    -- are dropped.  But if the write close happens, the elements in
    -- the queue are retained, and the queue isn't closed until all the
    -- elements have been read.  Writes will still fail in this "twilight
    -- time" however.
    --
    createUnboundedQueueDuct :: forall a m .
                                MonadUnliftIO m
                                => ControlThread m (Duct Simple m a)
    createUnboundedQueueDuct = 
        createQueueDuct TQueue.newTQueueIO TQueue.tryReadTQueue
                            TQueue.writeTQueue TQueue.flushTQueue
