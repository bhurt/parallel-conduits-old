{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct
-- Description : Parallel Conduit Ducts
-- Copyright   : (c) Brian Hurt, 2022
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything -- in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
-- This module introduces Ducts, which are like MVars but have an additional
-- Closed state.  In addition, we split a Duct into two endpoints- a read
-- endpoint and a write endpoint.  This makes Ducts more like unix pipes
-- in this way.
-- 
-- The problem with MVars is that there is no easy way to signal the
-- writers of the duct that the readers have all exited.  Previous attempts
-- of this library tried to use MVars with a side channel IORef to signal
-- that the readers had exited.  But this proved clunky.
--
-- This implementation uses STM.  The advantage of STM is that we can easily
-- define our own "MVar-plus" type around a single TVar holding a
-- `DuctState`.  Even more so, we can build up a new Duct endpoint
-- ontop of multiple other duct endpoints- see `writeTuple`, `writeEither`,
-- etc. for examples.
--
-- The problem with STM is that it does give rise to the "thundering herd"
-- problem- writing a single TVar and wake up many threads.  This is one
-- place where MVars shine- they guarantee a single wakeup.  However, in
-- the specific use case of parallel conduits, there is a simple solution.
-- We simply add "shim threads" in those places where the thundering herd
-- is likely to be a problem.  A shim thread either reads from one duct
-- and writes to multiple ducts, or reads from multiple ducts and writes
-- to one duct.
-- 
-- This allows us to guarantee that there is only ever a single thread on
-- either end of a duct (even though a given thread may be on the end of
-- multiple ducts).  In this library, it's not a problem, but this sharp
-- corner of ducts is why they will never be made into their own library.
--
module Data.Conduit.Parallel.Internal.Duct where

    import           Control.Concurrent.STM
    import           Data.Functor.Contravariant

    import           Data.Conduit.Parallel.Internal.Spawn

    -- | A duct, which is the pair of a read endpoint and a write endpoint.
    --
    -- A duct can be in one of three states:
    --
    --  [@Empty@] The duct currently does not have an element to read.
    --      Reads from the duct will retry, while writes will succeed.
    --  [@Full@] The duct currently does have an element to read.
    --      Reads from the duct will succeed, while write will retry.
    --  [@Closed@] The duct is closed.  Both reads and writes will fail
    --      in a way to indicate that the duct is closed.  Multiple
    --      attempts to read a closed duct are not an error, but should
    --      have no effect.  @Closed@ is the terminal state of a duct-
    --      once it has closed, it can never again be either @Full@ or
    --      @Empty@.
    --
    -- This structure could just be a tuple, but from experience,
    -- which ever order I return the two endpoints in, I'll screw
    -- it up about half the time.
    --
    data Duct m a =
        Duct {
            -- | Get the read endpoint of the duct.
            getReadEndpoint  :: ReadDuct m a,

            -- | Get the write endpoint of the duct.
            getWriteEndpoint :: WriteDuct m a }


    -- | The read endpoint of a duct.
    --
    -- This is an STM action available as a resource in a worker thread.
    newtype ReadDuct m a =
        ReadDuct {
            getReadDuct :: WorkerThread m (STM (Maybe a)) }

    instance Functor m => Functor (ReadDuct m) where
        fmap f rd = ReadDuct $ fmap (fmap f) <$> getReadDuct rd


    data IsOpen =
        IsOpen
        | IsClosed
        deriving (Show, Read, Ord, Eq, Enum, Bounded)

    newtype WriteDuct m a =
        WriteDuct {
            getWriteDuct :: WorkerThread m (a -> STM IsOpen) }

    instance Functor m => Contravariant (WriteDuct m) where
        contramap f wd = WriteDuct $ (. f) <$> getWriteDuct wd

{-
    -- | Read endpoint of a duct.
    data ReadDuct a =
        ReadDuct {

            -- | Reads an element from the duct in the STM monad.
            --
            --      * If the duct is @Empty@ (i.e. has no element to read),
            --          then this function retries.
            --      * If the duct is @Full@ (i.e. has an element to read),
            --          then this function returns that element in a
            --          @Just@ and makes the duct empty.
            --      * If the duct is @Closed@, then this function simply
            --          returns @Nothing@.  Once this function returns
            --          @Nothing@, all further calls will also return 
            --          @Nothing@.  It is not an error to call this 
            --          function multiple times on a closed duct, it just 
            --          should not have any effect.
            --
            -- This interface is designed to work with the @MaybeT@
            -- monad transformer higher up the stack.  The behavior
            -- then becomes that if the duct is @Empty@, the function
            -- blocks, if @Full@ it returns the value, and if @Closed@,
            -- it aborts the whole operation.  This is useful behavior
            -- elsewhere.
            readDuctSTM :: WorkerSTM (Maybe a),

            -- | Closes the duct.  
            --
            --      * If the duct is @Empty@, the duct simply becomes @Closed@.
            --      * If the duct is @Full@, the element in the duct is
            --          discarded, and the duct is @Closed@.
            --      * If the duct is @Closed@, then the duct remains @Closed@.
            --          It is not an error to close an already closed
            --          duct.
            closeReadDuctSTM :: STM () }

    -- | @Applicative@ and @Alternative@ instances are probably possible,
    --      but I don't think they're useful.  I'm not sure a @Monad@
    --      instance is sane, and definitely isn't useful.
    instance Functor ReadDuct where
        fmap f rd = ReadDuct {
                        readDuctSTM = (fmap f <$> readDuctSTM rd),
                        closeReadDuctSTM = closeReadDuctSTM rd }

    -- | Write endpoint of a duct.
    data WriteDuct a =
       WriteDuct {
 
            -- | Writes an element to the duct in the STM monad.
            --
            --      * If the duct is @Empty@ (i.e. has no element to read),
            --          then this function makes the duct @Full@ holding
            --          the given element and returns @Just ()@.
            --      * If the duct is @Full@ (i.e. has an element to read),
            --          then this function retries.
            --      * If the duct is @Closed@, then this function simply
            --          returns @Nothing@.  Once this function returns
            --          @Nothing@, all further calls will also return 
            --          @Nothing@.  It is not an error to call this 
            --          function multiple times on a closed duct, it just 
            --          should not have any effect.
            --
            -- This interface is designed to work with the @MaybeT@
            -- monad transformer higher up the stack.  See the `ReadDuct`
            -- documentation for why.
            writeDuctSTM :: a -> STM (Maybe ()),

            -- | Closes the duct.  
            --
            --      * If the duct is @Empty@, the duct simply becomes @Closed@.
            --      * If the duct is @Full@, this function retries.  This is
            --          different from the behavior of `closeReadSTM`.  By
            --          retrying (blocking), this allows the downstream
            --          readers to read and handle the last element before
            --          the close takes effect.
            --      * If the duct is @Closed@, then the duct remains @Closed@.
            --          It is not an error to close an already closed
            --          duct.
            closeWriteSTM :: STM () }

    -- | WriteDucts are contravariant functors.
    --
    -- Note: WriteDucts are consumers, not producers.
    instance Contravariant WriteDuct where
        contramap f wr = WriteDuct {
                            writeDuctSTM = writeDuctSTM wr . f,
                            closeWriteSTM = closeWriteSTM wr }

-}
