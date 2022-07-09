
module Main where

    import qualified Fuse
    import qualified Functor
    import           Test.HUnit

    allTests :: Test
    allTests = TestLabel "allTests" $
                    TestList [
                        Fuse.fuseTests,
                        Functor.functorTests
                    ]

    main :: IO ()
    main = runTestTTAndExit allTests

