{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct.Utils
-- Description : Miscellaneous usefull stuff for Ducts.
-- Copyright   : (c) Brian Hurt, 2022
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything -- in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Duct.Utils where

    import           Control.Concurrent.STM
    import           Control.Exception

    -- | Exception to throw when a closed duct is encountered.
    --
    -- It is common  when combining ducts to want to roll back changes
    -- to previous ducts when we encounter a closed duct.  We can
    -- do this by wrapping all the changes with a @catchSTM@, and then
    -- doing a @throwSTM@ when we encounter a closed duct.
    --
    -- This is the value to throw to do this.
    data ClosedDuctException = ClosedDuctException
        deriving (Show)

    instance Exception ClosedDuctException


    -- | Convert a retry into a Nothing.
    --
    -- Another common thing to want to do is to catch when a STM action
    -- does a retry.  This function returns @Nothing@ when the STM action
    -- retries, and @Just a@ when it does not.
    catchRetry :: forall a .  STM a -> STM (Maybe a)
    catchRetry act = (Just <$> act) `orElse` pure Nothing



