{-# LANGUAGE ScopedTypeVariables #-}

-- | Testing the fuse functions
module Fuse(
    fuseTests
) where

    import           Data.Conduit.Parallel
    import           Data.List             (sort)
    import           Data.Monoid           (All (..))
    import           GHC.Stack
    import           Test.HUnit
    import           Util

    -- | Export all the tests out of this module.
    fuseTests :: HasCallStack => Test
    fuseTests = TestLabel "fuseTests" $
                    TestList [
                        testFuse,
                        testFuseLeft,
                        testFuseS,
                        testFuseWith
                    ]

    -- | Test that basic fuse, and runParConduit, work.
    testFuse :: HasCallStack => Test
    testFuse = TestLabel "testFuse" $
                    TestCase $ do
                        res :: [ Int ]
                            <- runParConduit $
                                fuse
                                    (intSource 10 ())
                                    listSink
                        [1..10] @=? sort res

    -- | Test fuseLeft
    testFuseLeft :: HasCallStack => Test
    testFuseLeft = testConduit "testFuseLeft" $
                    fuseLeft
                        (intSource 2 True)
                        (expectSink () [1..2])

    -- | Test fuseS
    testFuseS :: HasCallStack => Test
    testFuseS = TestLabel "testFuseS" $
                    TestCase $ do
                        assert $
                            getAll <$>
                                (runParConduit
                                    (fuseS
                                        (intSource 2 (All True))
                                        (expectSink (All True) [1..2])))

    -- | Test fuseWith
    testFuseWith :: HasCallStack => Test
    testFuseWith = testConduit "testFuseS" $
                    fuseWith (&&)
                        (intSource 2 True)
                        (expectSink True [1..2])

