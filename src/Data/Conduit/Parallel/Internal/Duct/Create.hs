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
-- instead.  Anything -- in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Duct.Create where

    import           Control.Monad.STM
    import           UnliftIO

    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Duct

    data DuctState a =
        Empty
        | Full a
        | Closed

    createDuct :: forall a m . MonadUnliftIO m => ControlThread m (Duct m a)
    createDuct = go <$> newTVarIO Empty
        where
            go :: TVar (DuctState a) -> Duct m a
            go tvar = Duct {
                        getReadEndpoint = readEndpoint tvar,
                        getWriteEndpoint = writeEndpoint tvar }

            readEndpoint :: TVar (DuctState a) -> ReadDuct m a
            readEndpoint tvar = ReadDuct $ workerThreadBracket
                                            (doRead tvar)
                                            (readClose tvar)

            doRead :: TVar (DuctState a) -> m (STM (Maybe a))
            doRead tvar = pure $ do
                            s <- readTVar tvar
                            case s of
                                Empty -> retry
                                Full a -> do
                                    writeTVar tvar Empty
                                    pure $ Just a
                                Closed -> pure Nothing

            readClose :: TVar (DuctState a) -> STM (Maybe a) -> m ()
            readClose tvar _ = do
                _ <- safeAtomically $ writeTVar tvar Closed
                pure ()

            writeEndpoint :: TVar (DuctState a) -> WriteDuct m a
            writeEndpoint tvar = WriteDuct$ workerThreadBracket
                                            (doWrite tvar)
                                            (writeClose tvar)

            doWrite :: TVar (DuctState a) -> m (a -> STM IsOpen)
            doWrite tvar = pure $ \a -> do
                s <- readTVar tvar
                case s of
                    Empty -> do
                        writeTVar tvar (Full a)
                        pure IsOpen
                    Full _ -> retry
                    Closed -> pure IsClosed

            writeClose :: TVar (DuctState a)
                            -> (a -> STM IsOpen)
                            -> m ()
            writeClose tvar _ = do
                _ <- safeAtomically $ do
                        s <- readTVar tvar
                        case s of
                            Empty  -> writeTVar tvar Closed
                            Full _ -> retry
                            Closed -> pure ()
                pure ()

