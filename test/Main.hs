
module Main where

    import qualified Fuse
    import qualified Functor
    import qualified Route
    import           Test.HUnit

    allTests :: Test
    allTests = TestLabel "allTests" $
                    TestList [
                        Fuse.fuseTests,
                        Functor.functorTests,
                        Route.routeTests
                    ]

    main :: IO ()
    main = runTestTTAndExit allTests

