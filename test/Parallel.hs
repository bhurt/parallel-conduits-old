{-# LANGUAGE ScopedTypeVariables #-}

-- | Testing parallel and friends.
module Parallel (
    parallelTests
) where

    import           Data.Conduit.Parallel
    import           GHC.Stack
    import           Test.HUnit
    import           Util


    parallelTests :: HasCallStack => Test
    parallelTests = TestLabel "parallelTests" $
                        TestList [
                            parallelTest
                        ]

    ints :: [ Int ]
    ints = [ 1 .. 20 ]

    parallelTest :: HasCallStack => Test
    parallelTest = testConduit "parallelTest" $
                        listSource ints ()
                        `fuse`
                        parallel 3 randomDelay
                        `fuse`
                        expectSink True ints
                        
