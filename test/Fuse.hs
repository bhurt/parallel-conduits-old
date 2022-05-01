{-# LANGUAGE ScopedTypeVariables #-}

-- | Testing the fuse functions
module Fuse(
    fuseTests
) where

    import qualified Data.Conduit.List     as C
    import           Data.Conduit.Parallel
    import           Data.List             (sort)
    import           Data.Monoid           (All (..))
    import           Data.Void
    import           GHC.Stack
    import           Test.HUnit

    -- | Export all the tests out of this module.
    fuseTests :: HasCallStack => Test
    fuseTests = TestLabel "fuseTests" $
                    TestList [
                        testFuse,
                        testFuseLeft,
                        testFuseS
                    ]

    -- | Test that basic fuse, and runParConduit, work.
    testFuse :: HasCallStack => Test
    testFuse = TestLabel "testFuse" $
                    TestCase $ do
                        let inputs :: [ Int ]
                            inputs = [ 1 .. 10 ]
                            src :: ParConduit IO () () Int
                            src = liftConduit $ C.sourceList inputs
                            sink :: ParConduit IO [ Int ] Int Void
                            sink = liftConduit C.consume
                            cond :: ParConduit IO [ Int ] () Void
                            cond = fuse src sink
                        res :: [ Int ] <- runParConduit cond
                        sort inputs @=? sort res

    -- | Test fuseLeft
    testFuseLeft :: HasCallStack => Test
    testFuseLeft = TestLabel "testFuseLeft" $
                    TestCase $ do
                        let inputs :: [ Int ]
                            inputs = [ 1 .. 2 ]
                            src :: ParConduit IO Bool () Int
                            src = liftConduit $ True <$ C.sourceList inputs
                            sink :: ParConduit IO () Int Void
                            sink = liftConduit $ C.sinkNull
                            cond :: ParConduit IO Bool () Void
                            cond = fuseLeft src sink
                        assert $ runParConduit cond

    -- | Test fuseS
    testFuseS :: HasCallStack => Test
    testFuseS = TestLabel "testFuseS" $
                    TestCase $ do
                        let inputs :: [ Int ]
                            inputs = [ 1 .. 2 ]
                            src :: ParConduit IO All () Int
                            src = liftConduit $ All True <$ C.sourceList inputs
                            sink :: ParConduit IO All Int Void
                            sink = liftConduit $ All True <$ C.sinkNull
                            cond :: ParConduit IO All () Void
                            cond = fuseS src sink
                        assert $ getAll <$> runParConduit cond

