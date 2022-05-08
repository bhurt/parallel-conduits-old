{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Arrow.Type
-- Description : Create ParArrow Values
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
module Data.Conduit.Parallel.Internal.Arrow.Create where

    import           Control.DeepSeq
    import           Control.Monad.IO.Unlift
    import           UnliftIO.Exception      (evaluate)

    import           Data.Conduit.Parallel.Internal.Arrow.Type
    import           Data.Conduit.Parallel.Internal.Copy
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn

    fromFunction :: forall m a b .
                        (MonadUnliftIO m
                        , NFData b)
                        => (a -> b)
                        -> ParArrow m a b
    fromFunction f = fromKleisli (pure . f)

    fromKleisli :: forall m a b .
                    (MonadUnliftIO m
                    , NFData b)
                    => (a -> m b)
                    -> ParArrow m a b
    fromKleisli f = ParArrow go
        where
            go :: ReadDuct m a
                    -> WriteDuct m b
                    -> ControlThread m (m ())
            go rd wd = spawn $ modifyThread g rd wd

            g :: a -> m b
            g a = do
                b <- f a
                liftIO $ evaluate $ force b
