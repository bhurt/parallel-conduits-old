{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Lazy
-- Description : Parallel Conduit Ducts
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
-- Home for the Lazy data type.
--
module Data.Conduit.Parallel.Internal.Lazy where

    import           Control.DeepSeq

    -- | Wrapper to give a NFData instance for all types.
    --
    -- The functions that create parallel conduits from conduits or
    -- parallel arrows from arrows require the types that are output
    -- implement the NFData type class.  This allows the values to
    -- be forced in the thread that creates them.
    --
    -- This is normally the right thing to do.  And making it the
    -- default prevents a large number of errors where values are
    -- not forced in the right thread.  However, some types
    -- simply do not have a (sane) NFData implementation.  Also,
    -- some times moving the computation from on thread to another
    -- thread via unforced lazy evaluation is the desired effect.
    --
    -- The @Lazy@ type is the escape hatch for those situations. 
    -- Wrapping a value in @Lazy@ gives it an implementation of
    -- NFData that does not force the underlying value.
    newtype Lazy a = Lazy { getLazy :: a }

    instance NFData (Lazy a) where
        rnf _ = ()

