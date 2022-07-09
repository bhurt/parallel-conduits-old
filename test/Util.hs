{-# LANGUAGE ScopedTypeVariables #-}

-- | Common functions and conduits for testing.
module Util (
    listSource,
    intSource,
    listSink,
    discardSink,
    expectSink,
    testConduit
) where

    import           Control.DeepSeq
    import           Data.Conduit
    import qualified Data.Conduit.List     as C
    import           Data.Conduit.Parallel
    import           GHC.Stack
    import           Test.HUnit

    -- | Given a list of elements, just output them.
    --
    -- You also supply the result value.
    --
    -- This is just a wrapper around the sourceList conduit.
    --
    listSource :: forall r x . NFData x => [x] -> r -> ParConduit IO r () x
    listSource xs r = liftConduit $ r <$ C.sourceList xs

    -- | Produce a list of Ints 1 to n.
    --
    -- This is just a wrapper around `listSource`.
    intSource :: forall r . Int -> r -> ParConduit IO r () Int
    intSource n = listSource [ 1 .. n ]

    -- | Consume all the elements, and return them in a list.
    --
    -- This is just a wrapper around the consume conduit.
    listSink :: forall x . ParConduit IO [ x ] x Void
    listSink = liftConduit C.consume

    -- | Consume and discard all the inputs.
    --
    -- You also specify the constant value to return.
    --
    -- This is just a wrapper around the sinkNull conduit.
    discardSink :: forall x r . r -> ParConduit IO r x Void
    discardSink r = liftConduit $ r <$ C.sinkNull

    -- | Compare inputs to a given list of expected values.
    --
    -- Elements are removed from the list as they are seen.  But if
    -- duplicate elements are expected, they can be duplicated in the
    -- list multiple times.  So, if you expect the value 1 to be produced
    -- three times, you'd just have three different 1's in the list.
    -- Then, if only two 1's or four 1's are seen, that would be an error
    -- (five is right out).
    --
    -- Values in the list that aren't seen are also errors.
    --
    -- If values not in the list are seen, or values in the list are not
    -- seen, then an error is thrown.
    --
    expectSink :: forall x r . (Eq x, Show x)
                    => r -> [ x ] -> ParConduit IO r x Void
    expectSink r inits = liftConduit (expectCond inits)
        where
            expectCond :: [x] -> ConduitT x Void IO r
            expectCond xs = do
                mx :: Maybe x <- await
                case mx of
                    Nothing ->
                        case xs of
                            [] -> pure r
                            _ -> error $ "Expected values not seen: "
                                                ++ show xs
                    Just x -> expectCond (removeFirst x xs)

            removeFirst :: x -> [x] -> [x]
            removeFirst x (y: ys)
                | (y == x) = ys
                | otherwise = y : removeFirst x ys
            removeFirst x [] = error $ "Unexpected value " ++ show x


    -- Test the execution of a single ParConduit.
    --
    -- Just eliminating common duplicate code.
    --
    testConduit :: HasCallStack
                    => String
                    -> ParConduit IO Bool () Void
                    -> Test
    testConduit label cond =
        TestLabel label $
            TestCase $
                assert $ runParConduit cond
