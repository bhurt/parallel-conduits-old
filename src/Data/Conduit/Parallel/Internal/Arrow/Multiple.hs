{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Arrow.Type
-- Description : Create ParArrow Values
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
module Data.Conduit.Parallel.Internal.Arrow.Multiple(
    multiple
) where

    import           Data.Sequence          (Seq)
    import qualified Data.Sequence          as Seq
    import           UnliftIO

    import           Data.Conduit.Parallel.Internal.Arrow.Type
    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Duct.Utils
    import           Data.Conduit.Parallel.Internal.Spawn

    data Foo  m a b = Foo {
        input :: WriteDuct m a,
        thread :: m (),
        output :: ReadDuct m b
    }

    multiple :: forall m a b .
                MonadUnliftIO m
                => Int
                -> ParArrow m a b
                -> ParArrow m a b
    multiple n base
        | n < 1     = error "Multiple must be >= 1"
        | n == 1    = base
        | otherwise = ParArrow go
            where
                go :: ReadDuct m a
                        -> WriteDuct m b
                        -> ControlThread m (m ())
                go rd wd = do
                    foos :: [ Foo m a b ]
                        <- sequence $ replicate n makeFoo

                    let inputs :: [ WriteDuct m a ]
                        inputs = input <$> foos

                        allInputs :: WriteDuct m a
                        allInputs = makeSeqWrite inputs

                        outputs :: [ ReadDuct m b ]
                        outputs = output <$> foos

                        allOutputs :: ReadDuct m b
                        allOutputs = makeSeqRead outputs

                        threads :: m ()
                        threads = sequence_ $ thread <$> foos

                    m1 :: m () <- spawn $ copyThread rd allInputs
                    m2 :: m () <- spawn $ copyThread allOutputs wd

                    pure $ m1 >> m2 >> threads

                makeFoo :: ControlThread m (Foo m a b)
                makeFoo = do
                    ind :: Duct m a <- createSimpleDuct 
                    outd :: Duct m b <- createSimpleDuct
                    thrd :: m () <- getParArrow base
                                        (getReadEndpoint ind)
                                        (getWriteEndpoint outd)
                    pure $ Foo {
                        input = getWriteEndpoint ind,
                        thread = thrd,
                        output = getReadEndpoint outd }

                makeSeqWrite :: [ WriteDuct m a ] -> WriteDuct m a 
                makeSeqWrite wds = WriteDuct $ goWrite wds

                goWrite :: [ WriteDuct m a ]
                            -> WorkerThread m (a -> STM (Maybe ()))
                goWrite wds = do
                    wrs :: [ a -> STM (Maybe ()) ]
                        <- mapM getWriteDuct wds
                    tvar <- newTVarIO (Seq.fromList wrs)
                    enforceClosed (\g -> \a -> g (doWrite tvar a))

                doWrite :: TVar (Seq (a -> STM (Maybe ())))
                            -> a
                            -> STM (Maybe ())
                doWrite tvar a = do
                    sq :: Seq (a -> STM (Maybe ())) <- readTVar tvar
                    case Seq.viewl sq of
                        Seq.EmptyL  -> pure Nothing
                        x Seq.:< xs -> do
                            r :: Maybe () <- x a
                            case r of
                                Nothing -> do
                                    -- This should be rare, so it's not
                                    -- worth optimizing.
                                    writeTVar tvar xs
                                    doWrite tvar a
                                Just () -> do
                                    writeTVar tvar (xs Seq.|> x)
                                    pure $ Just ()


                makeSeqRead :: [ ReadDuct m b ] -> ReadDuct m b
                makeSeqRead rds = ReadDuct $ goRead rds

                goRead :: [ ReadDuct m b ] -> WorkerThread m (STM (Maybe b))
                goRead rds = do
                    rs :: [ STM (Maybe b) ] <- mapM getReadDuct rds
                    tvar :: TVar (Seq (STM (Maybe b)))
                        <- newTVarIO $ Seq.fromList rs
                    enforceClosed $ \g -> g (doRead tvar)

                doRead :: TVar (Seq (STM (Maybe b)))
                            -> STM (Maybe b)
                doRead tvar = do
                    sq :: Seq (STM (Maybe b)) <- readTVar tvar
                    case Seq.viewl sq of
                        Seq.EmptyL  -> pure Nothing
                        x Seq.:< xs -> do
                            r :: Maybe b <- x
                            case r of
                                Nothing -> do
                                    writeTVar tvar xs
                                    doRead tvar
                                Just b -> do
                                    writeTVar tvar (xs Seq.|> x)
                                    pure $ Just b


