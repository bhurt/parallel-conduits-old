{-# LANGUAGE ScopedTypeVariables #-}

-- | Testing the various functor implementations.
module Functor(
    functorTests
) where

    import qualified Control.Category      as Cat
    import           Data.Conduit.Parallel
    import           Data.Profunctor
    import           Data.Void             (Void)
    import           GHC.Stack
    import           Test.HUnit
    import           Util
    import           Witherable

    -- | The standard list of ints.
    --
    -- Useful to pass to both `listSource` and `expectSink`.
    ints :: [ Int ]
    ints = [ 1 .. 10 ]


    -- | The standard list of ints converted to strings via show.
    --
    -- Useful for passing to `expectSink`, so we can use show as a
    -- mapping function.
    strings :: [ String ]
    strings = show <$> ints


    produceInts :: ParConduit IO () () Int
    produceInts = listSource ints ()

    consumeInts :: ParConduit IO Bool Int Void
    consumeInts = expectSink True ints

    consumeStrings :: ParConduit IO Bool String Void
    consumeStrings = expectSink True strings

    -- | Export all the tests out of this module.
    functorTests :: HasCallStack => Test
    functorTests = TestLabel "functorTests" $
                        TestList [
                                testFmap,
                                testDimap,
                                testLmap,
                                testRmap,
                                testCatMaybes,
                                testComp,
                                testMapResult
                            ]

    -- | Test the fmap function
    testFmap :: HasCallStack => Test
    testFmap = testConduit "testFmap" $
                    (fmap show produceInts)
                    `fuse`
                    consumeStrings


    -- | Test the dimap function.
    testDimap :: HasCallStack => Test
    testDimap = testConduit "testDimap" $
                    let middle :: ParConduit IO () Int Int
                        middle = dimap show read Cat.id
                    in
                    produceInts
                    `fuse`
                    middle
                    `fuse`
                    consumeInts

    -- | Test the lmap function
    testLmap :: HasCallStack => Test
    testLmap = testConduit "testLmap" $
                    produceInts
                    `fuse`
                    (lmap show consumeStrings)

    -- | Test the rmap function
    testRmap :: HasCallStack => Test
    testRmap = testConduit "testRmap" $
                    (rmap show produceInts)
                    `fuse`
                    consumeStrings

    -- | Test the catMaybes function
    testCatMaybes :: HasCallStack => Test
    testCatMaybes = testConduit "testCatMaybes" $
                        catMaybes maybeInts
                        `fuse`
                        expectSink True expectResults
        where
            maybeInts :: ParConduit IO () () (Maybe Int)
            maybeInts = myMap <$> produceInts

            myMap :: Int -> Maybe Int
            myMap i
                | myTest i  = Just i
                | otherwise = Nothing

            expectResults :: [ Int ]
            expectResults = Prelude.filter myTest ints

            myTest :: Int -> Bool
            myTest i = (i `mod` 2) == 0

    -- | Test the Category composition operator.
    testComp :: HasCallStack => Test
    testComp = testConduit "testComp" $
                    (produceInts Cat.. Cat.id)
                    `fuse`
                    consumeInts


    -- | Test the mapResult function.
    testMapResult :: HasCallStack => Test
    testMapResult = testConduit "testMapResult" $
                        produceInts
                        `fuse`
                        mapResult not (expectSink False ints)

