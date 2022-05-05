{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Arrow.Type
-- Description : The ParArrow type
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
-- Provide the main @ParArrow@ type, and associated type classes.
module Data.Conduit.Parallel.Internal.Arrow.Type where

    import           Control.Applicative
    import qualified Control.Arrow                        as A
    import qualified Control.Category                     as Cat
    import           Control.Monad.IO.Unlift
    import qualified Control.Selective                    as Sel
    import           Data.Functor.Contravariant
    import           Data.Functor.Contravariant.Divisible
    import           Data.Profunctor
    import           Data.These

    import           Data.Conduit.Parallel.Internal.Arrow.Utils
    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Utils

    -- | A Parallel Arrow
    newtype ParArrow m a b =
        ParArrow {
            getParArrow :: ReadDuct m a
                            -> WriteDuct m b
                            -> ControlThread m (m ())
        }

    makeParConduit :: ParArrow m a b -> ParConduit m () a b
    makeParConduit = ParConduit . getParArrow

    instance Functor m => Functor (ParArrow m a) where
        fmap f pc = ParArrow $
                        \rd wd -> getParArrow pc rd (contramap f wd)

    instance Functor m => Profunctor (ParArrow m) where
        dimap f g pc = ParArrow $
                        \rd wd -> getParArrow pc (fmap f rd)
                                        (contramap g wd)
        lmap f pc = ParArrow $
                        \rd wd -> getParArrow pc (fmap f rd) wd
        rmap g pc = ParArrow $
                        \rd wd -> getParArrow pc rd (contramap g wd)

    instance MonadUnliftIO m => Applicative (ParArrow m a) where
        pure a = ParArrow $ \rd wd -> do
                                m1 <- spawn $ discardThread rd
                                m2 <- spawn $ spamThread wd a
                                pure $ m1 >> m2
        a1 <*> a2 = ParArrow $ splice
                                (\() () -> ()) 
                                (divide (\a -> (a,a)))
                                (<*>)
                                (getParArrow a1)
                                (getParArrow a2)

    instance MonadUnliftIO m => Choice (ParArrow m) where
        left' :: forall a b c .
                    ParArrow m a b
                    -> ParArrow m (Either a c) (Either b c)
        left' pa = ParArrow $ bypass f1 (getParArrow pa)
            where
                f1 :: Either a c -> Either (Either b c) (a, b -> Either b c)
                f1 (Left a)  = Right (a, Left)
                f1 (Right c) = Left (Right c)

        right' :: forall a b c .
                    ParArrow m a b
                    -> ParArrow m (Either c a) (Either c b)
        right' pa = ParArrow $ bypass f1 (getParArrow pa)
            where
                f1 :: Either c a -> Either (Either c b) (a, b -> Either c b)
                f1 (Left c) = Left (Left c)
                f1 (Right a) = Right (a, Right)

    instance MonadUnliftIO m => Cat.Category (ParArrow m) where
        id = ParArrow $ \rd wr -> spawn (copyThread rd wr)
        pc1 . pc2 = ParArrow $ fuseInternal
                                    mappend
                                    (getParArrow pc2)
                                    (getParArrow pc1)

    instance MonadUnliftIO m => Sel.Selective (ParArrow m i) where
        select :: forall a b .
                    ParArrow m i (Either a b)
                    -> ParArrow m i (a -> b)
                    -> ParArrow m i b
        select pae paf = ParArrow go
            where
                go :: ReadDuct m i
                        -> WriteDuct m b
                        -> ControlThread m (m ())
                go rdi wdb = do
                    educt :: Duct m (Either b (i, a)) <- createSimpleDuct
                    mu1 :: m () <- bypass f1 (getParArrow pae)
                                        rdi (getWriteEndpoint educt)

                    mu2 :: m () <- bypass f2 (getParArrow paf)
                                        (getReadEndpoint educt) wdb
                    pure $ mu1 >> mu2

                f1 :: i -> Either (Either b (i, a))
                                (i, (Either a b -> (Either b (i, a))))
                f1 i = Right (i, f1a i)

                f1a :: i -> Either a b -> Either b (i, a)
                f1a i (Left a)  = Right (i, a)
                f1a _ (Right b) = Left b

                f2 :: Either b (i, a) -> Either b (i, (a -> b) -> b)
                f2 (Left b) = Left b
                f2 (Right (i, a)) = Right (i, (\t -> t a))

    instance MonadUnliftIO m => A.Arrow (ParArrow m) where
        arr :: forall b c . (b -> c) -> ParArrow m b c
        arr f = ParArrow go
            where
                go :: ReadDuct m b
                        -> WriteDuct m c
                        -> ControlThread m (m ())
                go rd wd = spawn $ copyThread (f <$> rd) wd

        first :: forall b c d . ParArrow m b c -> ParArrow m (b, d) (c, d)
        first inner = ParArrow $ bypass f (getParArrow inner)
            where
                f :: (b, d) -> Either (c, d) (b, c -> (c, d))
                f (b, d) = Right (b, (\c -> (c, d)))

        second :: forall b c d . ParArrow m b c -> ParArrow m (d, b) (d, c)
        second inner = ParArrow $ bypass f (getParArrow inner)
            where
                f :: (d, b) -> Either (d, c) (b, c -> (d, c))
                f (d, b) = Right (b, \c -> (d, c))

        (***) :: forall b c b' c' .
                    ParArrow m b c
                    -> ParArrow m b' c'
                    -> ParArrow m (b, b') (c, c')
        (***) pa1 pa2 = ParArrow $ splice (<>) (divide id) (liftA2 (,))
                                    (getParArrow pa1) (getParArrow pa2)

        (&&&) :: forall b c c' .
                    ParArrow m b c
                    -> ParArrow m b c'
                    -> ParArrow m b (c, c')
        (&&&) pa1 pa2 = ParArrow $
                            splice (<>) (writeThose f) (liftA2 (,))
                                (getParArrow pa1) (getParArrow pa2)
                where
                    f :: b -> These b b
                    f b = These b b

    instance MonadUnliftIO m => A.ArrowChoice (ParArrow m) where

        left :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (Either b d) (Either c d)
        left inner = ParArrow $ bypass f (getParArrow inner)
            where
                f :: Either b d -> Either (Either c d) (b, c -> Either c d)
                f (Right d) = Left (Right d)
                f (Left b)  = Right (b, Left)

        right :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (Either d b) (Either d c)
        right inner = ParArrow $ bypass f (getParArrow inner)
            where
                f :: Either d b -> Either (Either d c) (b, c -> Either d c)
                f (Left d) = Left (Left d)
                f (Right b) = Right (b, Right)

        (+++) :: forall b c b' c' .
                    ParArrow m b c
                    -> ParArrow m b' c'
                    -> ParArrow m (Either b b') (Either c c')
        (+++) p1 p2 = ParArrow $ dispatch dir (getParArrow p1)
                                                (getParArrow p2)
            where
                dir :: Either b b' ->
                        Either (b, c -> Either c c') (b', c' -> Either c c')
                dir (Left b) = Left (b, Left)
                dir (Right b') = Right (b', Right)

        (|||) :: forall b c d .
                    ParArrow m b d
                    -> ParArrow m c d
                    -> ParArrow m (Either b c) d
        (|||) p1 p2 = ParArrow $ dispatch dir (getParArrow p1)
                                                (getParArrow p2)
            where
                dir :: Either b c -> Either (b, d -> d) (c, d -> d)
                dir (Left b) = Left (b, id)
                dir (Right c) = Right (c, id)

