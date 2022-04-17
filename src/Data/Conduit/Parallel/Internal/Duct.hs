{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct
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
-- This module introduces Ducts, which are like MVars but have an additional
-- Closed state.  In addition, we split a Duct into two endpoints- a read
-- endpoint and a write endpoint.  This makes Ducts more like unix pipes
-- in this way.
--
-- = Motivation
-- 
-- The problem with MVars is that there is no easy way to signal the
-- writers of the duct that the readers have all exited.  Previous attempts
-- of this library tried to use MVars with a side channel IORef to signal
-- that the readers had exited.  But this proved clunky.
--
-- This implementation uses STM.  The advantage of STM is that we can easily
-- define our own "MVar-plus" type around a single TVar holding a
-- @DuctState@.  Even more so, we can build up a new Duct endpoint
-- ontop of multiple other duct endpoints- see
-- `Data.Conduit.Parallel.Internal.Duct.Write.writeTuple`,
-- `Data.Conduit.Parallel.Internal.Duct.Write.writeEither`,
-- etc. for examples.
--
-- The problem with STM is that it does give rise to the "thundering herd"
-- problem- writing a single TVar and wake up many threads.  This is one
-- place where MVars shine: they guarantee a single wakeup.  However, in
-- the specific use case of parallel conduits, there is a simple solution.
--
-- We simply add "shim threads" in those places where the thundering herd
-- is likely to be a problem.  A shim thread either reads from one duct
-- and writes to multiple ducts, or reads from multiple ducts and writes
-- to one duct.
-- 
-- This allows us to guarantee that there is only ever a single thread on
-- either end of a duct (even though a given thread may be on the end of
-- multiple ducts).  In this library, it's not a problem, but this sharp
-- corner of ducts is why they will never be made into their own library.
--
-- = Duct Flavors
--
-- Ducts endpoints come in two flavors:
--
--  [@Simple@] endpoints are where reading from or writing to the duct
--      is cheap- reading or writing a TVar or less.
--  [@Complex@] endpoints are where reading from or writing to the
--      duct can be expensive.  Duct endpoints which are constructed
--      out of other duct endpoints are @Complex@.
--
-- == Motivation for Flavors
--
-- The difference is that it's not worth it to avoid re-reading or
-- re-writing @Simple@ endpoints.  This is important when we're
-- combining multiple endpoints into a single endpoint- if we combine
-- multiple @Complex@ endpoints together can have surprising performance
-- problems.  So we avoid it by only combining @Simple@ endpoints.  And
-- we use a phantom type variable to enforce this.
--
module Data.Conduit.Parallel.Internal.Duct where

    import           Control.Concurrent.STM
    import           Data.Functor.Contravariant

    import           Data.Conduit.Parallel.Internal.Spawn

    -- | The flavor of a duct endpoint, lifted to a kind.
    --
    -- We're using the DataKinds extension in this module, so
    -- Flavor is also a Kind.
    data Flavor =
        SimpleFlavor    -- ^ Simple duct endpoints
        | ComplexFlavor -- ^ Complex duct endpoints

    -- | Phantom Type for Simple Endpoints
    --
    -- Note that DataKinds-defined types can't be exported from a
    -- module.  Only the data type is exported.  Which means that
    -- the importing module needs to have DataKinds enabled as
    -- well.  But by making a type synonym, we /can/ export that.
    --
    -- So the DataKinds type is @SimpleFlavor@, the type synomym
    -- is @Simple@, and I just use @Simple@ everywhere else without
    -- having to enable DataKinds.
    --
    type Simple = 'SimpleFlavor

    -- | Phantom Type for Complex Endpoints
    --
    -- See `Simple` for an explanation of what is going on here.
    --
    type Complex = 'ComplexFlavor

    -- | A duct, which is the pair of a read endpoint and a write endpoint.
    --
    -- This structure could just be a tuple, but from experience,
    -- which ever order I return the two endpoints in, I'll screw
    -- it up about half the time.
    --
    data Duct (t :: Flavor) m a =
        Duct {
            -- | Get the read endpoint of the duct.
            getReadEndpoint  :: ReadDuct t m a,

            -- | Get the write endpoint of the duct.
            getWriteEndpoint :: WriteDuct t m a }


    -- | The read endpoint of a duct.
    --
    -- This is an STM action available as a resource in a worker thread.
    -- The STM action resource will behave the following way:
    -- 
    --  * If the duct is not closed and contains at least one value,
    --      that value will be removed from the duct and returned from
    --      the STM action in a @Just@.
    --  * If the duct is not closed but does not contain even one value,
    --      the STM action will retry.
    --  * If the duct is closed, the STM action will return @Nothing@.
    --
    -- Once the worker thread exits (and the STM action is released)
    -- the duct will be closed.
    --
    newtype ReadDuct (t :: Flavor) m a =
        ReadDuct {
            getReadDuct :: WorkerThread m (STM (Maybe a)) }

    instance Functor m => Functor (ReadDuct t m) where
        fmap f rd = ReadDuct $ fmap (fmap f) <$> getReadDuct rd


    -- | The write endpoint of a duct.
    --
    -- This is an STM action available as a resource in a worker thread.
    -- The STM action resource will behave the following way:
    -- 
    --  * If the duct is not closed and can take at least one more
    --      value, the value is added to the duct and @Just ()@ is
    --      returned from the STM action.
    --  * If the duct is not closed but can not take even one more
    --      value, then the STM action will retry.
    --  * If the duct is closed, the STM action will return @Nothing@.
    --
    -- Once the worker thread exits (and the STM action is released)
    -- the duct will be closed.
    --
    newtype WriteDuct (t :: Flavor) m a =
        WriteDuct {
            getWriteDuct :: WorkerThread m (a -> STM (Maybe ())) }

    instance Functor m => Contravariant (WriteDuct t m) where
        contramap f wd = WriteDuct $ (. f) <$> getWriteDuct wd

