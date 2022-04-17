-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct.No
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
-- Read and write endpoints for ducts that are already closed.  Useful for
-- implementing parallel conduits that should not be read and/or write
-- values, i.e. sources and sinks.
--
module Data.Conduit.Parallel.Internal.Duct.No where

    import           Data.Void

    import           Data.Conduit.Parallel.Internal.Duct


    -- | Pre-closed read conduit
    --
    -- This is a read endpoint for a duct that is already closed.  This
    -- is useful for situations where a parallel conduit should not read
    -- any values (i.e. it is a source).  It's special cased because
    -- it can be more efficiently implemented.
    noRead :: ReadDuct Simple m ()
    noRead = ReadDuct $ pure (pure Nothing)

    -- | Pre-closed write conduit.
    --
    -- This is a write endpoint for a duct that is already closed.  This
    -- is useful for situaitons where a parallel conduit should not write
    -- any values (i.e. it is a sink).  It's special cased because it can
    -- be more efficiently implemented.
    --
    noWrite :: WriteDuct Simple m Void
    noWrite = WriteDuct $ pure absurd



