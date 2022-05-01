
module Main where

    import qualified Fuse
    import           Test.HUnit

    allTests :: Test
    allTests = TestLabel "allTests" $
                    TestList [
                        Fuse.fuseTests
                    ]

    main :: IO ()
    main = runTestTTAndExit allTests

