{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Types
-- Description : Parallel Conduit Types
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
module Data.Conduit.Parallel.Internal.Types where

    import qualified Control.Category           as Cat
    import           Control.Monad.IO.Unlift
    import           Data.Functor.Contravariant
    import           Data.Profunctor

    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Utils

    newtype ParConduit m r i o =
        ParConduit {
            getParConduit :: ReadDuct Simple m i
                                -> WriteDuct Simple m o
                                -> ControlThread m (m r) }

    instance Functor m => Functor (ParConduit m r i) where
        fmap f pc = ParConduit $
                        \rd wd -> getParConduit pc rd (contramap f wd)


    instance Functor m => Profunctor (ParConduit m r) where
        dimap f g pc = ParConduit $
                        \rd wd -> getParConduit pc (fmap f rd)
                                        (contramap g wd)

        lmap f pc = ParConduit $
                        \rd wd -> getParConduit pc (fmap f rd) wd

        rmap g pc = ParConduit $
                        \rd wd -> getParConduit pc rd (contramap g wd)

    instance (MonadUnliftIO m, Monoid r)
        => Cat.Category (ParConduit m r) where
            id = ParConduit $ \rd wr -> fmap mempty <$> spawn (copyThread rd wr)
            pc1 . pc2 =
                ParConduit $ fuseInternal
                                mappend
                                (getParConduit pc2)
                                (getParConduit pc1)


