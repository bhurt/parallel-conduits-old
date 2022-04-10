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
module Data.Conduit.Parallel.Internal.Duct.No where

    import           Data.Void

    import           Data.Conduit.Parallel.Internal.Duct


    noRead :: ReadDuct Simple m ()
    noRead = ReadDuct $ pure (pure Nothing)

    noWrite :: WriteDuct Simple m Void
    noWrite = WriteDuct $ pure absurd



