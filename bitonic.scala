package bitonic

import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable.ArrayBuffer

object Bitonic {
	
	class BitonicSortArgs {
		var m_startIndex : Int = 0	          	
		var m_size : Int = 0			 		
		var m_directionUp : Boolean = true		
		var m_hybridSort : Boolean = false		
		var m_level : Int = 0            		
	}

	class BitonicSortActor extends Actor {
		def act(){
			loop {
				react {
					case "STOP" => {
						if(mailboxSize > 0) {
							reply(false)
						} else {
							reply(true)
							this.exit
						}
					}
					case (msg:BitonicSortArgs) => {
						if (msg.m_size > 1){
							if (msg.m_level > 0){
								val half : Int = msg.m_size / 2;
								var half1 : Int = half
								val half2 : Int = half

								if(msg.m_size % 2 == 1)
									half1 += 1

								val bitonicActor1 = new BitonicSortActor
								val sort_args1 = new BitonicSortArgs

								sort_args1.m_startIndex = msg.m_startIndex
								sort_args1.m_size = half1
								sort_args1.m_directionUp = msg.m_directionUp
								sort_args1.m_hybridSort = msg.m_hybridSort
								sort_args1.m_level = msg.m_level -1

								for (i <- 0 to half1 - 1 )
									bitonicActor1.m_cont += m_cont(i)

								bitonicActor1.start()
								bitonicActor1 ! sort_args1

								var done1 : Boolean = (bitonicActor1 !? "STOP").asInstanceOf[Boolean]
								while(done1 == false) {
									println("Actor still working!")
									done1 = (bitonicActor1 !? "STOP").asInstanceOf[Boolean]
								}

								val bitonicActor2 = new BitonicSortActor
								val sort_args2 = new BitonicSortArgs

								sort_args2.m_startIndex = msg.m_startIndex
								sort_args2.m_size = half2
								sort_args2.m_directionUp = !msg.m_directionUp
								sort_args2.m_hybridSort = msg.m_hybridSort
								sort_args2.m_level = msg.m_level - 1

								for (i <- half1 to m_cont.size - 1 )
								bitonicActor2.m_cont += m_cont(i)

								bitonicActor2.start()
								bitonicActor2 ! sort_args2

								var done2 : Boolean = (bitonicActor2 !? "STOP").asInstanceOf[Boolean]
								while(done2 == false) {
									println("Actor still working!")
									done2 = (bitonicActor2 !? "STOP").asInstanceOf[Boolean]
								}

								m_cont.clear
								m_cont.appendAll(bitonicActor2.m_cont)
								m_cont.appendAll(bitonicActor1.m_cont)

								bitonic_merge(0, msg.m_size, msg.m_directionUp, m_cont)
							} else {
								if(msg.m_hybridSort)
									m_cont.sortWith(_ < _) //works with scala 2.8
								else
									bitonic_sort(msg.m_startIndex, msg.m_size, msg.m_directionUp, m_cont);
							}
						}
					}
				}
			}
		}

		val m_cont : ArrayBuffer[Int] = new ArrayBuffer[Int]

		private def bitonic_compare(i : Int, j : Int, dir : Boolean, vec:ArrayBuffer[Int]) {
			if(dir == (vec(i) > vec(j))) {
				val tmp = vec(i)
				vec(i) = vec(j)
				vec(j) = tmp
			}
		}

		private def bitonic_merge(lo : Int, n : Int, dir : Boolean, vec:ArrayBuffer[Int]) {
			if (n > 1) {
				val m = pp2(n);
				for (i <- lo to (lo + n - m - 1))
					bitonic_compare(i, i+m, dir, vec)

				bitonic_merge(lo, m, dir, vec)
				bitonic_merge(lo+m, n-m, dir, vec)
			}
		}

		private def bitonic_sort(lo : Int, n : Int, dir : Boolean, vec:ArrayBuffer[Int]) {
			if(n > 1) {
				val m = n/2
				bitonic_sort(lo, m, !dir, vec)
				bitonic_sort(lo+m, n-m, dir, vec)
				bitonic_merge(lo, n, dir, vec)
			}
		}

		//power of 2 that is less than n
		private def pp2(n : Int) = {
			var k : Int = 1
			while(k < n)
				k = k << 1
			k >> 1
		}
	}

	def main(args:Array[String]) {
		val st = System.currentTimeMillis
		val bitonicActor =  new BitonicSortActor

		bitonicActor.start()

		val n = 100000

		for (i <- 1 to n) {
			bitonicActor.m_cont += n + 1 - i
		}
		
		println(" -------- ")
		println("Printing first 10 elements before sorting :")
		for (i <- 0 to 9) {
			println( "Element " + i + " : " + bitonicActor.m_cont(i))
		}

		val sort_args = new BitonicSortArgs
		sort_args.m_startIndex = 0;
		sort_args.m_size = bitonicActor.m_cont.size;
		sort_args.m_directionUp = true;
		sort_args.m_hybridSort = false;
		sort_args.m_level = 8;

		bitonicActor ! sort_args

		var done : Boolean = (bitonicActor !? "STOP").asInstanceOf[Boolean]
		while(done == false) {
			println("Actor still working!")
			done = (bitonicActor !? "STOP").asInstanceOf[Boolean]
		}

		println(" -------- ")
		println("Printing first 10 elements after sorting :")
		for (i <- 0 to 9) {
			println( "Element " + i + " : " + bitonicActor.m_cont(i))
		}
		
		val et = System.currentTimeMillis
		println( "Overall processing time: " + ( et - st ) + "ms" )
	}
}