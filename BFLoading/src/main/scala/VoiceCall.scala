case class VoiceCall(
                      a: String,
                      b: String,
                      c: Long,
                      d: Int,
                      e: Int,
                      f: Long,
                      g: Int,
                      aa: String,
                      bb: Long,
                      cc: Int,
                      dd: String,
                      ee: Long,
                      ff: Long,
                      gg: Long,
                      h: Long,
                      i: String,
                      j: String,
                      k: Int,
                      l: String,
                      m: String,
                      n: String,
                      hh: String,
                      ii: String,
                      jj: Int,
                      kk: Int,
                      ll: Long,
                      mm: Int,
                      nn: Long,
                      o: String,
                      p: Long,
                      q: Long,
                      r: Long,
                      s: Int,
                      t: Int,
                      u: Int,
                      v: Int,
                      w: Int,
                      oo: Int,
                      pp: Long,
                      qq: Int,
                      rr: Long,
                      ss: Int,
                      tt: Long,
                      u: Int,
                      v: Int,
                      w: Int,
                      x: String,
                      y: Int,
                      z: Int,
                      uu: String,
                      vv: String,
                      ww: Int,
                      xx: Int,
                      yy: Long,
                      zz: Long,
                      aaa: String,
                      bbb: String,
                      ccc: String  
                    ) {
  def this(attributes: Array[String]) = {

    this(attributes(0), attributes(1), TransferUtil.LongTransfer(attributes(2).trim), TransferUtil.intTransfer(attributes(3).trim),
      TransferUtil.intTransfer(attributes(4).trim), TransferUtil.LongTransfer(attributes(5).trim), TransferUtil.intTransfer(attributes(6).trim),
      attributes(7), TransferUtil.LongTransfer(attributes(8).trim), TransferUtil.intTransfer(attributes(9).trim), attributes(10),
      TransferUtil.LongTransfer(attributes(11).trim), TransferUtil.LongTransfer(attributes(12).trim), TransferUtil.LongTransfer(attributes(13).trim),
      TransferUtil.LongTransfer(attributes(14).trim), attributes(15), attributes(16), TransferUtil.intTransfer(attributes(17).trim),
      attributes(18), attributes(19), attributes(20), attributes(21), attributes(22), TransferUtil.intTransfer(attributes(23).trim),
      TransferUtil.intTransfer(attributes(24).trim), TransferUtil.LongTransfer(attributes(25).trim), TransferUtil.intTransfer(attributes(26).trim),
      TransferUtil.LongTransfer(attributes(27).trim), attributes(28), TransferUtil.LongTransfer(attributes(29).trim), TransferUtil.LongTransfer(attributes(30).trim),
      TransferUtil.LongTransfer(attributes(31).trim), TransferUtil.intTransfer(attributes(32).trim), TransferUtil.intTransfer(attributes(33).trim),
      TransferUtil.intTransfer(attributes(34).trim), TransferUtil.intTransfer(attributes(35).trim), TransferUtil.intTransfer(attributes(36).trim),
      TransferUtil.intTransfer(attributes(37).trim), TransferUtil.LongTransfer(attributes(38).trim), TransferUtil.intTransfer(attributes(39).trim),
      TransferUtil.LongTransfer(attributes(40).trim), TransferUtil.intTransfer(attributes(41).trim), TransferUtil.LongTransfer(attributes(42).trim),
      TransferUtil.intTransfer(attributes(43).trim), TransferUtil.intTransfer(attributes(44).trim), TransferUtil.intTransfer(attributes(45).trim), attributes(46),
      TransferUtil.intTransfer(attributes(47).trim), TransferUtil.intTransfer(attributes(48).trim), attributes(49), attributes(50),
      TransferUtil.intTransfer(attributes(51).trim), TransferUtil.intTransfer(attributes(52).trim), TransferUtil.LongTransfer(attributes(53).trim),
      TransferUtil.LongTransfer(attributes(54).trim), attributes(55), attributes(56), TransferUtil.patitionKeyTransfer(attributes(8))
    )
  }

}