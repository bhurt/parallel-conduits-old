{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Conduit.Parallel
-- Description : Implementation of the parallel function
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
module Data.Conduit.Parallel.Internal.Conduit.Parallel where

    import           Control.Concurrent.STM
    import           Control.Monad.IO.Unlift
    import           Data.List.NonEmpty      (NonEmpty (..))
    import qualified Data.List.NonEmpty      as NonEmpty
    import           Data.Semigroup
    import           Data.Sequence           (Seq, ViewL (..), (|>))
    import qualified Data.Sequence           as Seq
    import           Data.Void               (Void)

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Duct.No
    import           Data.Conduit.Parallel.Internal.Spawn

    -- | Run multiple parallel conduits.
    --
    --
    -- Pictorially, @parallel inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/parallel.svg example>>
    -- 
    parallel :: forall m i o r .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => Int
                    -> ParConduit m r i o
                    -> ParConduit m r i o
    parallel n pc
        | n <= 0    = error "Data.Conduit.Parallel.parallel n <= 0 not allowed."
        | n == 1    = pc
        | otherwise = ParConduit go
            where
                go :: ReadDuct m i
                        -> WriteDuct m o
                        -> ControlThread m (m r)
                go rdi wdo = do
                    (mwdi, mrdis) <- createMultiRead n
                    (mwdos, mrdo) <- createMultiWrite n

                    let foo :: NonEmpty (ControlThread m (m r))
                        foo = NonEmpty.zipWith (getParConduit pc)
                                    mrdis mwdos
                    bar :: NonEmpty (m r) <- sequence foo
                    let baz :: m (NonEmpty r)
                        baz = sequence bar
                        qux :: m r
                        qux = sconcat <$> baz

                    mu1 :: m () <- spawn $ copyThread rdi mwdi
                    mu2 :: m () <- spawn $ copyThread mrdo wdo

                    pure $ mu1 >> mu2 >> qux

    -- | A more efficient version of `parallel` for sources.
    --
    --
    -- Pictorially, @heads inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/heads.svg example>>
    -- 
    heads :: forall m o r .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => Int
                    -> ParConduit m r () o
                    -> ParConduit m r () o
    heads n pc
        | n <= 0    = error "Data.Conduit.Parallel.heads n <= 0 not allowed."
        | n == 1    = pc
        | otherwise = ParConduit go
            where
                go :: ReadDuct m ()
                        -> WriteDuct m o
                        -> ControlThread m (m r)
                go _ wdo = do
                    (mwdos, mrdo) <- createMultiWrite n

                    let foo :: NonEmpty (ControlThread m (m r))
                        foo = getParConduit pc noRead <$> mwdos
                    bar :: NonEmpty (m r) <- sequence foo
                    let baz :: m (NonEmpty r)
                        baz = sequence bar
                        qux :: m r
                        qux = sconcat <$> baz

                    mu :: m () <- spawn $ copyThread mrdo wdo

                    pure $ mu >> qux

    -- | A more efficient version of `parallel` for sinks.
    --
    --
    -- Pictorially, @tails inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/tails.svg example>>
    -- 
    tails :: forall m i r .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => Int
                    -> ParConduit m r i Void
                    -> ParConduit m r i Void
    tails n pc
        | n <= 0    = error "Data.Conduit.Parallel.tails n <= 0 not allowed."
        | n == 1    = pc
        | otherwise = ParConduit go
            where
                go :: ReadDuct m i
                        -> WriteDuct m Void
                        -> ControlThread m (m r)
                go rdi _ = do
                    (mwdi, mrdis) <- createMultiRead n

                    let foo :: NonEmpty (ControlThread m (m r))
                        foo = (\rd -> getParConduit pc rd noWrite) <$> mrdis
                    bar :: NonEmpty (m r) <- sequence foo
                    let baz :: m (NonEmpty r)
                        baz = sequence bar
                        qux :: m r
                        qux = sconcat <$> baz

                    mu :: m () <- spawn $ copyThread rdi mwdi

                    pure $ mu >> qux

    createMultiRead :: forall a m .
                        MonadUnliftIO m
                        => Int
                        -> ControlThread m 
                            (WriteDuct m a, NonEmpty (ReadDuct m a))
    createMultiRead n
            | n <= 0    = error "Invalid number of readDucts in createMultiRead"
            | otherwise = do
                emptyList :: TVar (Maybe (Seq (TVar (DuctState a))))
                    <- liftIO $ newTVarIO $ Just Seq.empty
                liveCount :: TVar Int
                    <- liftIO $ newTVarIO $ n
                let rd :: ReadDuct m a
                    rd = makeReadDuct emptyList liveCount

                    rds :: NonEmpty (ReadDuct m a)
                    rds = rd :| replicate (n - 1) rd
                pure (makeWriteDuct emptyList, rds)

        where
            closeEmptyList :: TVar (Maybe (Seq (TVar (DuctState a))))
                                -> STM ()
            closeEmptyList emptyList = do
                oldState :: Maybe (Seq (TVar (DuctState a)))
                    <- readTVar emptyList
                case oldState of
                    Nothing -> pure ()
                    Just q  -> do
                        writeTVar emptyList Nothing
                        mapM_ (\t -> writeTVar t Closed) q

            addElement :: TVar (Maybe (Seq (TVar (DuctState a))))
                            -> a -> STM (Maybe ())
            addElement emptyList a = do
                oldState :: Maybe (Seq (TVar (DuctState a)))
                    <- readTVar emptyList
                case oldState of
                    Nothing -> pure Nothing
                    Just q ->
                        case Seq.viewl q of
                            EmptyL      -> retry
                            tvar :< remq -> do
                                writeTVar emptyList $ Just remq
                                s <- readTVar tvar
                                case s of
                                    -- It is possible for tvars in the empty
                                    -- list to be closed.  If this happens,
                                    -- we just drop them and try again.
                                    Closed ->
                                        addElement emptyList a
                                    Full _ -> -- This should never happen
                                        addElement emptyList a
                                    Empty -> do
                                        writeTVar tvar (Full a)
                                        pure $ Just ()

            makeReadDuct :: TVar (Maybe (Seq (TVar (DuctState a))))
                            -> TVar Int
                            -> ReadDuct m a
            makeReadDuct emptyList liveCount = ReadDuct go
                where
                    go :: WorkerThread m (STM (Maybe a))
                    go = do
                        tvar <- createTVar
                        workerThreadBracket
                            (pure (doRead tvar))
                            (const (readClose tvar))

                    createTVar :: WorkerThread m (TVar (DuctState a))
                    createTVar = liftIO $ do
                        -- create tvar
                        tvar <- newTVarIO Empty
                        -- add it to the end of the empty list
                        _ <- safeAtomically $ addToEmptyList tvar
                        pure tvar

                    addToEmptyList :: TVar (DuctState a) -> STM ()
                    addToEmptyList tvar = do
                        e :: Maybe (Seq (TVar (DuctState a)))
                            <- readTVar emptyList
                        case e of
                            Nothing -> writeTVar tvar Closed
                            Just q  -> writeTVar emptyList $
                                            Just (q |> tvar)

                    doRead :: TVar (DuctState a) -> STM (Maybe a)
                    doRead tvar = do
                        s :: DuctState a <- readTVar tvar
                        case s of
                            -- Note that we only sample our own tvar before
                            -- retrying- this way, changes to the empty
                            -- list don't wake us up.
                            Empty  -> retry

                            -- Note: now that we're full, we don't retry.
                            -- So sampling the emptyList isn't a problem.
                            Full a -> do
                                writeTVar tvar Empty
                                addToEmptyList tvar
                                pure $ Just a

                            Closed -> pure Nothing

                    readClose :: TVar (DuctState a) -> m ()
                    readClose tvar = do
                        -- We update the count in it's own transaction, so
                        -- this takes effect immediately even if the second
                        -- transaction retries.
                        r' <- safeAtomically $ do
                                modifyTVar' liveCount (\x -> x - 1)
                                readTVar liveCount
                        let r = case r' of
                                    Nothing -> 1
                                    Just x  -> x
                        _ <- safeAtomically $ do
                            if (r > 0)
                            then do -- There are live read channels still
                                -- If we have an element in our tvar,
                                -- we need to at least try to give it to
                                -- some other read duct.  Note that this
                                -- can retry, if no other read duct is empty
                                -- at the moment.  Eventually either
                                -- some other read duct will become empty,
                                -- allowing us to pass along our value, or
                                -- the whole duct will close, kicking us
                                -- off the retry.
                                t <- readTVar tvar
                                writeTVar tvar Closed
                                case t of
                                    Empty  -> pure ()
                                    Closed -> pure ()
                                    Full a -> do
                                        _ <- addElement emptyList a
                                        pure ()
                            else do -- We are the last read channel.
                                -- Any element we're holding in our TVar
                                -- is dropped (there is no read duct that
                                -- is still open to receive it).
                                writeTVar tvar Closed
                                closeEmptyList emptyList
                        pure ()

            makeWriteDuct :: TVar (Maybe (Seq (TVar (DuctState a))))
                                -> WriteDuct m a
            makeWriteDuct emptyList = WriteDuct go
                where
                    go :: WorkerThread m (a -> STM (Maybe ()))
                    go = workerThreadBracket
                            (pure (addElement emptyList))
                            (const (safeAtomically (closeEmptyList emptyList)))

    createMultiWrite :: forall a m .
                        MonadUnliftIO m
                        => Int
                        -> ControlThread m
                            (NonEmpty (WriteDuct m a), ReadDuct m a)
    createMultiWrite n
            | n <= 0    = error "Invalid number of readDucts in createMultiRead"
            | otherwise = do
                fullList :: TVar (Maybe (Seq (TVar (DuctState a))))
                    <- liftIO $ newTVarIO $ Just Seq.empty
                liveCount :: TVar Int
                    <- liftIO $ newTVarIO $ n
                let wd :: WriteDuct m a
                    wd = makeWriteDuct fullList liveCount
                    wds :: NonEmpty (WriteDuct m a)
                    wds = wd :| replicate (n - 1) wd
                pure (wds, makeReadDuct fullList liveCount)

        where
            makeWriteDuct :: TVar (Maybe (Seq (TVar (DuctState a))))
                                -> TVar Int
                                -> WriteDuct m a
            makeWriteDuct fullList liveCount = WriteDuct go
                where
                    go :: WorkerThread m (a -> STM (Maybe ()))
                    go = do
                            tvar :: TVar (DuctState a)
                                <- liftIO $ newTVarIO $ Empty
                            workerThreadBracket
                                (pure (doWrite tvar))
                                (const (closeWrite tvar))

                    doWrite :: TVar (DuctState a) -> a -> STM (Maybe ())
                    doWrite tvar a = do
                        s :: DuctState a <- readTVar tvar
                        case s of
                            Full _ -> retry
                            Closed -> pure Nothing
                            Empty  -> do
                                t :: Maybe (Seq (TVar (DuctState a)))
                                    <- readTVar fullList
                                case t of
                                    Nothing -> do
                                        writeTVar tvar Closed
                                        pure Nothing
                                    Just q -> do
                                        writeTVar tvar (Full a)
                                        writeTVar fullList $
                                            Just (q |> tvar)
                                        pure $ Just ()

                    closeWrite :: TVar (DuctState a) -> m ()
                    closeWrite tvar = do
                        -- It doesn't matter if we're the last open
                        -- write duct or not.  We don't clean up
                        -- the full list.  We just can't close our
                        -- write duct until it's emptied.
                        _ <- safeAtomically $
                            modifyTVar' liveCount (\x -> x - 1)
                        _ <- safeAtomically $ do
                            t <- readTVar tvar
                            case t of
                                Full _ -> retry
                                Empty  -> writeTVar tvar Closed
                                Closed -> pure ()
                        pure ()


            makeReadDuct :: TVar (Maybe (Seq (TVar (DuctState a))))
                                -> TVar Int
                                -> ReadDuct m a
            makeReadDuct fullList liveCount = ReadDuct go
                where
                    go :: WorkerThread m (STM (Maybe a))
                    go = workerThreadBracket
                            (pure doRead)
                            (const closeRead)

                    doRead :: STM (Maybe a)
                    doRead = do
                        s :: Maybe (Seq (TVar (DuctState a)))
                            <- readTVar fullList
                        case s of
                            Nothing -> pure Nothing
                            Just q ->
                                case Seq.viewl q of
                                    EmptyL      -> do
                                        -- We only retry if there are
                                        -- still live writers (i.e. there
                                        -- is still hope of getting more
                                        -- values).
                                        c <- readTVar liveCount
                                        if (c > 0)
                                        then retry
                                        else do
                                            -- We are done.
                                            writeTVar fullList Nothing
                                            pure Nothing
                                    tvar :< remq -> do
                                        writeTVar fullList $ Just remq
                                        t <- readTVar tvar
                                        case t of
                                            -- It's possible for a tvar in
                                            -- full list to be closed.  If
                                            -- this happens, we just skip
                                            -- the tvar and move on.
                                            Closed -> doRead
                                            Empty  -> doRead
                                            Full a -> do
                                                writeTVar tvar Empty
                                                pure $ Just a

                    closeRead :: m ()
                    closeRead = do
                        _ <- safeAtomically $ do
                            -- Any values in any of the tvars
                            -- get dropped.
                            s :: Maybe (Seq (TVar (DuctState a)))
                                <- readTVar fullList
                            case s of
                                Nothing -> pure ()
                                Just q  -> do
                                    writeTVar fullList Nothing
                                    mapM_ (\t -> writeTVar t Closed) q
                        pure ()

