{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Conduit.Route
-- Description : Routing Functions for Parallel Conduits
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
-- Routing functions for Parallel Conduits.
module Data.Conduit.Parallel.Internal.Conduit.Route where

    import           Control.Monad.IO.Unlift
    import           Data.Functor.Contravariant.Divisible
    import           Data.Profunctor
    import           Data.These
    import           Data.Void

    import           Data.Conduit.Parallel.Internal.Conduit.Type
    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Duct.Create
    import           Data.Conduit.Parallel.Internal.Duct.No
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Utils

    -- | Divert @These@ inputs into one of two parallel conduits.
    --
    -- With the @This@ values being fed into the first parallel conduit,
    -- and the @That@ values being fed into the second.  Both conduits
    -- output values of type @c@ which are merged together.
    --
    -- The result of the combined parallel conduit is just the results
    -- of the two sub parallel conduits @<>@'ed together.  Normally
    -- the results are unit, which implements @Semigroup@, so this works
    -- as expected.
    --
    -- If both given conduits are sinks (i.e. they do not produce outputs),
    -- it is more efficient to use `routeThese`.
    --
    -- Pictorially, @splitThese c1 c2@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/splitThese.svg example>>
    -- 
    splitThese :: forall m r a b c .
                (MonadUnliftIO m
                , Semigroup r)
                => ParConduit m r a c
                -> ParConduit m r b c
                -> ParConduit m r (These a b) c
    splitThese ac bc = ParConduit $
                            splice (<>) (writeThose id) alternate
                                (getParConduit ac) (getParConduit bc)

    -- | Divert tuple inputs into one of two parallel conduits.
    --
    -- With the @fst@ values being fed into the first parallel conduit,
    -- and the @snd@ values being fed into the second.  Both conduits
    -- output values of type @c@ which are merged together.
    --
    -- The result of the combined parallel conduit is just the results
    -- of the two sub parallel conduits @<>@'ed together.  Normally
    -- the results are unit, which implements @Semigroup@, so this works
    -- as expected.
    --
    -- If both given conduits are sinks (i.e. they do not produce outputs),
    -- it is more efficient to use `routeTuple`.
    --
    -- Pictorially, @splitTuple c1 c2@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/splitTuple.svg example>>
    -- 
    splitTuple :: forall m r a b c .
                (MonadUnliftIO m
                , Semigroup r)
                => ParConduit m r a c
                -> ParConduit m r b c
                -> ParConduit m r (a, b) c
    splitTuple ac bc = ParConduit $
                            splice (<>) (divide id) alternate
                                (getParConduit ac) (getParConduit bc)

    -- | Divert @Either@ inputs into one of two parallel conduits.
    --
    -- With the @Left@ values being fed into the first parallel conduit,
    -- and the @Right@ values being fed into the second.  Both conduits
    -- output values of type @c@ which are merged together.
    --
    -- The result of the combined parallel conduit is just the results
    -- of the two sub parallel conduits @<>@'ed together.  Normally
    -- the results are unit, which implements @Semigroup@, so this works
    -- as expected.
    --
    -- If both given conduits are sinks (i.e. they do not produce outputs),
    -- it is more efficient to use `route`.
    --
    -- Pictorially, @split c1 c2@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/split.svg example>>
    -- 
    split :: forall m r a b c .
                (MonadUnliftIO m
                , Semigroup r)
                => ParConduit m r a c
                -> ParConduit m r b c
                -> ParConduit m r (Either a b) c
    split ac bc = ParConduit $
                            splice (<>) (choose id) alternate
                                (getParConduit ac) (getParConduit bc)


    -- | Merge a source into the current stream
    --
    -- Given a source, merge the values produced by the source into
    -- the current stream.  For values from the main stream, the segment
    -- acts as an id segment, in that inputs are passed unmodified to
    -- the output.
    --
    -- Pictorially, @merge inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/merge.svg example>>
    -- 
    merge :: forall m r a .
                MonadUnliftIO m
                => ParConduit m r () a
                -> ParConduit m r a  a
    merge inner = ParConduit $ go
        where
            go :: ReadDuct m a
                    -> WriteDuct m a
                    -> ControlThread m (m r)
            go rd wd = do
                d :: Duct m a <- createSimpleDuct
                mr :: m r <- getParConduit inner noRead (getWriteEndpoint d)
                let rd1 :: ReadDuct m a = alternate rd (getReadEndpoint d)
                mu :: m () <- spawn $ copyThread rd1 wd
                pure $ mu >> mr

    -- | Merge a source into the current stream, using Either.
    --
    -- This is just a common pattern around `merge`, where we tag the
    -- values produced by the @inner@ ParConduit with @Left@, and the
    -- values take in as input with @Right@.
    --
    -- In addition to being common, it also sort of the mirror image
    -- of `route`.
    --
    -- Pictorially, @mergeEither inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/mergeEither.svg example>>
    -- 
    mergeEither :: forall m r a b .
                    MonadUnliftIO m
                    => ParConduit m r () b
                    -> ParConduit m r a (Either b a)
    mergeEither inner = lmap Right $ merge (Left <$> inner)

    -- | The internal code for the various route functions.
    routeShared :: forall m r a b c .
                    MonadUnliftIO m
                    => (a -> These b c)
                    -> ParConduit m r b Void
                    -> ParConduit m r a c
    routeShared f inner = ParConduit $ go
        where
            go :: ReadDuct m a
                    -> WriteDuct m c
                    -> ControlThread m (m r)
            go rda wdc = do
                d :: Duct m b <- createSimpleDuct
                mr :: m r <- getParConduit inner (getReadEndpoint d) noWrite
                let wda = writeThose f (getWriteEndpoint d) wdc
                mu :: m () <- spawn $ copyThread rda wda
                pure $ mu >> mr

    -- | Route @These@ values into separate pipelines.
    --
    -- Takes input values of type @These b a@.  Routes the @This@ values
    -- to a separate pipeline, which must be a sink.  @That@ values
    -- get produced unchanged as it's output.
    --
    -- There is no mirror image @mergeThese@ of this function, as that
    -- would require synchronizing the outputs of two different parallel
    -- conduits.  Which doesn't make sense.  Consider using ParArrows
    -- (where this does make sense) if you need this.
    --
    -- Pictorially, @routeThese inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/routeThese.svg example>>
    -- 
    routeThese :: forall m r a b .
                MonadUnliftIO m
                => ParConduit m r b Void
                -> ParConduit m r (These b a) a
    routeThese = routeShared id

    -- | Route Tuple values into separate pipelines.
    --
    -- Takes input values of type @(b, a)@.  Routes the first value
    -- to a separate pipeline, which must be a sink.  The second value
    -- get produced unchanged as it's output.
    --
    -- There is no mirror image @mergeTuple@ of this function, as that
    -- would require synchronizing the outputs of two different parallel
    -- conduits.  Which doesn't make sense.  Consider using ParArrows
    -- (where this does make sense) if you need this.
    --
    -- Pictorially, @routeTuple inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/routeTuple.svg example>>
    -- 
    routeTuple :: forall m r a b .
                MonadUnliftIO m
                => ParConduit m r b Void
                -> ParConduit m r (b, a) a
    routeTuple = routeShared (\(b, a) -> These b a)

    -- | Route @Either@ values into separate pipelines.
    --
    -- Takes input values of type @Either b a@.  Routes the @Left@ values
    -- to a separate pipeline, which must be a sink.  @Right@ values
    -- get produced unchanged as it's output.
    --
    -- Sort of the mirror image of `mergeEither`.
    --
    -- This is very useful for error handling.
    --
    -- Pictorially, @route inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/route.svg example>>
    -- 
    route :: forall m r a b .
                MonadUnliftIO m
                => ParConduit m r b Void
                -> ParConduit m r (Either b a) a
    route = routeShared f
        where
            f :: Either b a -> These b a
            f (Left b)  = This b
            f (Right a) = That a

    -- | Duplicate values into a sink and also produce them as outputs.
    --
    -- Takes input values and sends them both as inputs into a sink
    -- and produces them as outputs.
    --
    -- Sort of the mirror image of `merge`.  See also
    -- `Data.Conduit.Parallel.parallel`.
    --
    -- Pictorially, @duplicate inner@ looks like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduits/master/docs/duplicate.svg example>>
    -- 
    duplicate :: forall m r a .
                    MonadUnliftIO m
                    => ParConduit m r a Void
                    -> ParConduit m r a a
    duplicate = routeShared f
        where
            f :: a -> These a a
            f a = These a a

