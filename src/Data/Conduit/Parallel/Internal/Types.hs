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
-- = Warning
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Types where

    import qualified Control.Category           as Cat
    import           Control.Monad.IO.Unlift
    import           Data.Functor.Contravariant
    import           Data.Profunctor

    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Utils

    -- | A ParConduit segment.
    --
    -- The type is modeled off the [Data.Conduit.ConduitT](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html#g:2) type.  We have
    -- four type variables:
    --
    --  * @m@ is the monad being executed in
    --
    --  * @r@ is the result type, produced when the segment exits.
    --
    --  * @i@ is the type of values being consumed, the "input" type
    --
    --  * @o@ is the type of values being produced, the "output" type
    --
    --
    -- Note that the type variables are in a different order than
    -- normal Conduits.  There are two reasons for this:
    --
    --  * Normal conduits get a lot of use of the fact that
    --      @Conduit i o@ is a monad transformer, and thus
    --      that @Conduit i o m@ is a monad.  This lets us write
    --      code that executes "inside" of a conduit.  Making
    --      a @ParConduit i o@ a monad transformer would have
    --      surprising implications (especially performance
    --      implications).
    --
    --  * Parallel Conduits get use out of being a @Profunctor@.
    --      instance.  This means that it is easy to map both
    --      the input and output types of a segment.  And it can
    --      get some use out of being a @Category@ (even if it
    --      isn't an @Arrow@).
    --
    -- There are two special types of segments:
    --
    -- [@Source@] A source is a segment of type @ParConduit m r () a@. 
    --              It produces values of type @a@ without consuming
    --              any inputs.  It is generally some form of unfold.
    --
    --  [@Sink@] A sink is a segment of the type @ParConduit m r a Void@. 
    --              It consumed values of type @a@ and produces no outputs. 
    --              It is generally some form of fold.
    --
    -- If a conduit is both a @source@ and a @sink@, i.e. if it
    -- has the type @ParConduit m r () Void@, then it is runnable.
    --
    -- We do not supply Applicative or Monad instances for this
    -- type- the implementation is difficult and the semantics would
    -- be complicated and surprising.  If you want to do this, do
    -- it with a normal Conduit, and then convert the Conduit into
    -- a ParConduit.  Or consider the `ParArrow` type.
    --
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


