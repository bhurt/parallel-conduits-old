{-# LANGUAGE ScopedTypeVariables #-}


-- |
-- Module      : Data.Conduit.Parallel.Internal.Conduit.Run
-- Description : Running Parallel Conduits
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
-- Provide the runParConduit function.

module Data.Conduit.Parallel.Internal.Conduit.Run where

    import           Data.Void

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Duct.No
    import           Data.Conduit.Parallel.Internal.Spawn

    -- | Run a parallel conduit
    --
    -- Given a parallel conduit segment that takes no inputs and gives
    -- no outputs, execute it.
    --
    -- Directly analogous to [Data.Conduit.runConduit](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html#v:runConduit).
    --
    runParConduit :: forall m r .
                        ParConduit m r () Void
                        -> m r
    runParConduit pc = runControl (getParConduit pc noRead noWrite) id


