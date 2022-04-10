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
module Data.Conduit.Parallel(

    -- * The ParConduit Type
    --
    -- | And running a parallel conduit.
    ParConduit
    -- , runParConduit,
) where

    import          Data.Conduit.Parallel.Internal.Types

    -- A comment on the image tags: I don't know how to include the image
    -- files as part of the documentation.  So instead, I (ab)use github
    -- as an image file server.  This will probably get me into trouble
    -- sooner or later.




