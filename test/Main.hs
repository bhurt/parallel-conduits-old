
module Main where

    import qualified Functor
    import qualified Fuse
    import qualified Parallel
    import qualified Route
    import           Test.HUnit

    allTests :: Test
    allTests = TestLabel "allTests" $
                    TestList [
                        Fuse.fuseTests,
                        Functor.functorTests,
                        Route.routeTests,
                        Parallel.parallelTests
                    ]

    main :: IO ()
    main = runTestTTAndExit allTests

