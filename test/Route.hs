{-# LANGUAGE ScopedTypeVariables #-}

-- | Testing the various routing functions.
module Route(
    routeTests
) where

    import qualified Control.Category      as Cat
    import           Data.Conduit.Parallel
    import           Data.Either
    import           Data.Profunctor
    import           Data.These
    import           GHC.Stack
    import           Test.HUnit
    import           Util


    routeTests :: HasCallStack => Test
    routeTests = TestLabel "routeTests" $
                    TestList [
                            splitTest,
                            splitTupleTest,
                            splitTheseTest,
                            mergeTest,
                            mergeEitherTest,
                            routeTheseTest,
                            routeTupleTest,
                            routeTest,
                            duplicateTest
                        ]

    ints :: [ Int ]
    ints = [ 1..10 ]

    simpleId :: ParConduit IO () Int Int
    simpleId = Cat.id

    splitTest :: HasCallStack => Test
    splitTest = testConduit "splitTest" $
                    listSource source ()
                    `fuse`
                    split simpleId (rmap read Cat.id)
                    `fuse`
                    expectSink True ints
        where
            source :: [ Either Int String ]
            source = fmap myMap ints

            myMap :: Int -> Either Int String
            myMap x
                | x `mod` 2 == 0 = Left x
                | otherwise      = Right $ show x


    splitTupleTest :: HasCallStack => Test
    splitTupleTest = testConduit "splitTupleTest" $
                        listSource source ()
                        `fuse`
                        splitTuple simpleId simpleId
                        `fuse`
                        expectSink True target
        where
            source :: [ (Int, Int) ]
            source = (\x -> (x, x)) <$> ints

            target :: [ Int ]
            target = ints >>= (\x -> [x, x])

    splitTheseTest :: HasCallStack => Test
    splitTheseTest = testConduit "splitTheseTest" $
                        listSource source ()
                        `fuse`
                        splitThese simpleId simpleId
                        `fuse`
                        expectSink True target
        where
            source :: [ These Int Int ]
            source = fmap makeThese ints

            makeThese :: Int -> These Int Int
            makeThese i =
                let n = i `mod` 3 in
                if (n == 0)
                then This i
                else
                    if (n == 1)
                    then That i
                    else These i i

            target :: [ Int ]
            target = ints >>= (\i -> if (i `mod` 3 == 2) then [i, i] else [i])


    mergeTest :: HasCallStack => Test
    mergeTest = testConduit "mergeTest" $
                    listSource ints ()
                    `fuse`
                    merge (listSource extras ())
                    `fuse`
                    expectSink True (ints ++ extras)
        where
            extras :: [ Int ]
            extras = (+100) <$> ints


    mergeEitherTest :: HasCallStack => Test
    mergeEitherTest = testConduit "mergeEitherTest" $
                    listSource ints ()
                    `fuse`
                    mergeEither (listSource extras ())
                    `fuse`
                    expectSink True res
        where
            extras :: [ String ]
            extras = show <$> ints

            res :: [ Either String Int ]
            res = (Right <$> ints) ++ (Left <$> extras)

    routeTheseTest :: HasCallStack => Test
    routeTheseTest = testConduit "routeTheseTest" $
                        listSource src ()
                        `fuse`
                        routeThese (expectSink True thisRes)
                        `fuse`
                        expectSink True thatRes
        where
            src :: [ These Int String ]
            src = foo <$> ints

            foo :: Int -> These Int String
            foo i =
                let m = i `mod` 3 in
                if (m == 0)
                then This i
                else
                    if (m == 1)
                    then That (show i)
                    else These i (show i)

            thisRes :: [ Int ]
            thisRes = src >>= getThis

            getThis :: These Int String -> [ Int ]
            getThis (This i)    = [ i ]
            getThis (That _)    = []
            getThis (These i _) = [ i ]

            thatRes :: [ String ]
            thatRes = src >>= getThat

            getThat :: These Int String -> [ String ]
            getThat (This _)    = []
            getThat (That s)    = [ s ]
            getThat (These _ s) = [ s ]

    routeTupleTest :: HasCallStack => Test
    routeTupleTest = testConduit "routeTupleTest" $
                        listSource src ()
                        `fuse`
                        routeTuple (expectSink True fstRes)
                        `fuse`
                        expectSink True sndRes
        where
            src :: [ (Int, String) ]
            src = (\i -> (i, show i)) <$> ints

            sndRes :: [ String ]
            sndRes = snd <$> src

            fstRes :: [ Int ]
            fstRes = fst <$> src

    routeTest :: HasCallStack => Test
    routeTest = testConduit "routeTest" $
                    listSource src ()
                    `fuse`
                    route (expectSink True leftRes)
                    `fuse`
                    expectSink True rightRes
        where
            src :: [ Either String Int ]
            src = go <$> ints

            go :: Int -> Either String Int
            go i = if (i `mod` 2 == 0)
                    then Left (show i)
                    else Right i

            leftRes :: [ String ]
            leftRes = lefts src

            rightRes :: [ Int ]
            rightRes = rights src

    duplicateTest :: HasCallStack => Test
    duplicateTest = testConduit "duplicateTest" $
                        listSource ints ()
                        `fuse`
                        duplicate (expectSink True ints)
                        `fuse`
                        expectSink True ints

