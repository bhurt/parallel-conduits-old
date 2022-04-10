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
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
-- = Purpose
--
-- This module is a collection of small bits of shared code between the
-- read and write duct modules.
--
-- A project either dies a hero, or live long to develop a Utils module.
--
module Data.Conduit.Parallel.Internal.Duct.Utils where

    import           Control.Concurrent.STM
    import           Control.Exception
    import           Control.Monad.IO.Class

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

    -- | Throw a `ClosedDuctException` is a duct is closed.
    --
    -- If the given STM action returns @Nothing@, throw a
    -- `ClosedDuctException`.  Otherwise, just return the value.
    --
    throwClosed :: forall a . STM (Maybe a) -> STM a
    throwClosed act = do
        r <- act
        case r of
            Just a  -> pure a
            Nothing -> throwSTM ClosedDuctException

    -- | Catch a `ClosedDuctException` and return closed.
    --
    -- If the given STM action throws a `ClosedDuctException`,
    -- catch it and return Nothing.  Otherwise, return the
    -- value.
    catchClosedDuct :: forall a . STM (Maybe a) -> STM (Maybe a)
    catchClosedDuct act = catchSTM act onE
        where
            onE :: ClosedDuctException -> STM (Maybe a)
            onE ClosedDuctException = pure Nothing


    -- | Convert a retry into a Nothing.
    --
    -- Another common thing to want to do is to catch when a STM action
    -- does a retry.  This function returns @Nothing@ when the STM action
    -- retries, and @Just a@ when it does not.
    catchRetry :: forall a .  STM a -> STM (Maybe a)
    catchRetry act = (Just <$> act) `orElse` pure Nothing


    -- | Enforce the constraint that once a duct is closed, it remains closed.
    --
    -- With complex ducts, it's possible for the duct to return that it
    -- is closed, and then accept more values.  For example, consider
    -- the case of @writeEither@, which creates a duct that accepts
    -- @Either a b@ values, and writes the value to an @a@ duct or
    -- a @b@ duct.  Then the @a@ closes, but the @b@ duct does not.
    -- So values of type @Left a@ will return that the duct is closed,
    -- but values of type @Right b@ will continue to be accepted.  This
    -- is obviously bad.
    --
    -- This function is the solution.  It keeps a @TVar@ with whether
    -- the duct has been closed.  Before calling the undering duct
    -- read or write function, it checks if the duct has closed.
    -- If it has, it returns @Nothing@ (i.e. closed).  If the duct
    -- isn't already closed, it calls the underlying STM action.
    -- And if that action returns the duct is closed, updates the @TVar@
    -- so that all future calls will also return Closed (regardless
    -- of whether the underlying STM action would have or not).
    --
    -- This started life as a performance, then morphed into a correctness
    -- enforcer.
    --
    -- The weird type signature is a function of the fact that we want
    -- to manage creating the @TVar@ that holds whether the duct is closed
    -- or not.
    --
    -- This function is also why write ducts return Maybe () instead
    -- of a boolean-equivalent.
    --
    enforceClosed :: forall a b m .
                            MonadIO m
                            => ((STM (Maybe a) -> STM (Maybe a)) -> b)
                            -> m b
    enforceClosed makeB = do
        isOpen <- liftIO $ newTVarIO True
        let f :: STM (Maybe a) -> STM (Maybe a)
            f act = do
                s <- readTVar isOpen
                case s of
                    False -> pure Nothing
                    True  -> do
                        r :: Maybe a <- act
                        case r of
                            Just a  -> pure $ Just a
                            Nothing -> do
                                writeTVar isOpen False
                                pure Nothing
        pure $ makeB f

