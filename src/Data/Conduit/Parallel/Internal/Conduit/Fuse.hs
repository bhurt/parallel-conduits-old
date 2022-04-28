{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Conduit.Fuse
-- Description : Fuse operations for parallel conduits.
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
-- Provide the various fuse* functions for parallel conduits.
module Data.Conduit.Parallel.Internal.Conduit.Fuse where

    import           Control.Monad.IO.Unlift

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Utils


    -- | Fuse two parallel conduits.
    --
    -- Directly analogous to the
    -- [normal Conduit fuse](https://hackage.haskell.org/package/conduit-1.3.4.2/docs/Data-Conduit.html#v:fuse).
    --
    -- Pictorially, we might represent the results of @fuse c1 c2@ like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/fuse.svg example>>
    --
    -- We are making one combined ParConduit from two sub ParConduits.  The 
    -- combined ParConduit takes input values of type @i@ and feeds them
    -- directly to the first sub ParConduit.  That first sub ParConduit
    -- produces intermediate values of type @x@, which gets feed as the
    -- input to the second sub ParConduit.  The second sub ParConduit
    -- then produces outputs of type @o@, which become the outputs of
    -- the combined ParConduit.
    --
    -- The result of the first sub ParConduit is discarded.  Normally,
    -- this will be @()@.  The result of the second sub ParConduit
    -- becomes the result of the combined ParConduit.
    fuse :: forall r1 r m i o x .
            MonadUnliftIO m
            => ParConduit m r1 i x
            -> ParConduit m r  x o
            -> ParConduit m r  i o
    fuse pc1 pc2 =
        ParConduit $
            fuseInternal
                (\_ r -> r)
                (getParConduit pc1)
                (getParConduit pc2)


    -- | Fuse two parallel conduits, taking the result from the first.
    --
    -- The same as `fuse`, except that the result is taken from the
    -- first sub ParConduit, not the second.
    --
    -- Pictorially, we might represent the results of @fuseLeft c1 c2@ like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/fuseLeft.svg example>>
    --
    -- We are making one combined ParConduit from two sub ParConduits.  The 
    -- combined ParConduit takes input values of type @i@ and feeds them
    -- directly to the first sub ParConduit.  That first sub ParConduit
    -- produces intermediate values of type @x@, which gets feed as the
    -- input to the second sub ParConduit.  The second sub ParConduit
    -- then produces outputs of type @o@, which become the outputs of
    -- the combined ParConduit.
    --
    -- The result of the second sub ParConduit is discarded.  Normally,
    -- this will be @()@.  The result of the first sub ParConduit
    -- becomes the result of the combined ParConduit.
    fuseLeft :: forall r1 r m i o x .
                MonadUnliftIO m
                => ParConduit m r  i x
                -> ParConduit m r1 x o
                -> ParConduit m r  i o
    fuseLeft pc1 pc2 =
        ParConduit $
            fuseInternal
                (\r _ -> r)
                (getParConduit pc1)
                (getParConduit pc2)

    -- | Fuse two parallel conduits, with semigroup results.
    --
    -- The same as `fuse`, except that the result is the @<>@
    -- (@mappend@, but for Semigroups) of the results of the two
    -- sub ParConduits.
    --
    -- The S in the name stands for Semigroup.
    --
    -- Pictorially, we might represent the results of @fuseM c1 c2@ like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/fuseS.svg example>>
    --
    -- We are making one combined ParConduit from two sub ParConduits.  The 
    -- combined ParConduit takes input values of type @i@ and feeds them
    -- directly to the first sub ParConduit.  That first sub ParConduit
    -- produces intermediate values of type @x@, which gets feed as the
    -- input to the second sub ParConduit.  The second sub ParConduit
    -- then produces outputs of type @o@, which become the outputs of
    -- the combined ParConduit.
    --
    -- The result of the combined ParConduit is the results of the two
    -- sub ParConduits @<>@ed together.
    --
    fuseS :: forall r m i o x .
                (MonadUnliftIO m
                , Semigroup r)
                => ParConduit m r i x
                -> ParConduit m r x o
                -> ParConduit m r i o
    fuseS pc1 pc2 =
        ParConduit $
            fuseInternal
                (<>)
                (getParConduit pc1)
                (getParConduit pc2)

