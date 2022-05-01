-- |
-- Module      : Data.Conduit.Parallel
-- Description : Parallel conduits, using Async, STM, and UnliftIO
-- Copyright   : (c) Brian Hurt, 2022
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- This library implements a parallel version of
-- the [Data.Conduit](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html)
-- library.  Each stage of conduit is executed in it's own thread,
-- concurrent with the other stages.  This allows the whole process to
-- overlap I/Os and use multiple cores to perform computation.  In
-- addition, the parallel conduit can tee, and perform multiple paths in
-- parallel.  This allows us to capture a large number of patterns of
-- parallel computation in a single library.
--
-- We provide a way to lift normal Conduits into parallel conduits.  This
-- allows us to access the rich ecosystem of the Conduit library.  The
-- Async library is used to spawn the threads, so exceptions are handled
-- correctly (exceptions that occur in any of the sub-threads are
-- propogated to the main thread, and all the other sub-threads are
-- cancelled).  We use UnliftIO so that many different monads can be
-- supported.
--
-- = Philosophy And Design Descisions
--
-- == Designed for Concurrency
--
-- This library is designed for concurrency, not parallelism.
--
-- Just to be clear here:
--
--  [@ Concurrency @] is doing a lot of different things all the same time
--  (handling web requests, doing database queries, etc).
--
--  [@ Parallelism @] is using lots of CPUs to do one thing fast
--  (invert a matrix, etc.).
--
--  [@ Multithreading @] is the union of parallelism and concurrency,
--  that is programs with multiple threads of control.
--
-- == Backpressure is important.
--
-- TODO: write this.
--
-- == Exceptions are Exceptional
--
-- TODO: write this
--
-- == Threads are (Relatively) Cheap
--
-- TODO: write this
--
-- == Fairness as a Default
--
-- When merging streams of values, we prefer the least recently used stream.
-- TODO: finish writing this.
--
-- == Strictness as a Default
--
-- Values get forced as they are produced.
--
module Data.Conduit.Parallel(

    -- * Parallel Conduits

    -- ** The ParConduit Type
    ParConduit


    -- ** Running a ParConduit
    , runParConduit

    -- ** Creating a ParConduit
    , liftConduit

    -- ** Mapping the Result
    , mapResult

    -- ** Fusing ParConduits
    , fuse
    , fuseLeft
    , fuseS

    -- ** ParConduit routing
    , split
    , splitTuple
    , splitThese
    , merge
    , mergeEither
    , route
    , routeThese
    , routeTuple
    , duplicate

    -- ** Multiple ParConduits
    , parallel
    , heads
    , tails

    -- ** Cache ParConduits
    , cache

    -- ** ParConduit Portals
    , portal

    -- * Parallel Arrows

    -- ** ParArrow Type
    , ParArrow

    -- * Lazy
    , Lazy(..)

) where

    import           Data.Conduit.Parallel.Internal.Arrow.Type
    import           Data.Conduit.Parallel.Internal.Conduit.Cache
    import           Data.Conduit.Parallel.Internal.Conduit.Create
    import           Data.Conduit.Parallel.Internal.Conduit.Fuse
    import           Data.Conduit.Parallel.Internal.Conduit.Parallel
    import           Data.Conduit.Parallel.Internal.Conduit.Portal
    import           Data.Conduit.Parallel.Internal.Conduit.Route
    import           Data.Conduit.Parallel.Internal.Conduit.Run
    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Lazy

    -- A comment on the image tags: I don't know how to include the image
    -- files as part of the documentation.  So instead, I (ab)use github
    -- as an image file server.  This will probably get me into trouble
    -- sooner or later.




