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

    import           Data.Functor.Contravariant
    import           Data.Profunctor

    -- import qualified Control.Category           as Cat
    -- import           Control.Monad.IO.Unlift
    -- import qualified Control.Selective          as Sel

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn

    -- import           Data.Conduit.Parallel.Internal.Copy
    -- import           Data.Conduit.Parallel.Internal.Utils

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

    {-
    instance MonadUnliftIO m => Applicative (ParArrow m a) where
        pure a = ParArrow $ \rd wd -> do
                                m1 <- spawn $ discardThread rd
                                m2 <- spawn $ spawnThread wd
                                pure $ m1 >> m2
        a1 <*> a2 = ParArrow $ splice
                                (\() () -> ()) 
                                writeDuplicate
                                (readBoth ($))
                                (getParArrow a1)
                                (getParArrow a2)

    instance MonadUnliftIO m => Choice (ParArrow m) where
        left' pa = ParArrow $ bypass route (getParArrow pa)
            where
                route (Right c) = Left (Right c)
                route (Left a) = Right (a, Left)
        right' pa = ParArrow $ bypass route (getParArrow pa)
            where
                route (Left c)  = Left (Left c)
                route (Right a) = Right (a, Right)
        

    instance MonadUnliftIO m => Cat.Category (ParArrow m) where
        id = ParArrow $ \rd wr -> spawn (copyThread rd wr)
        pc1 . pc2 = ParArrow $ fuseInternal
                                    mappend
                                    (getParArrow pc2)
                                    (getParArrow pc1)

    -}


