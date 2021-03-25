defmodule :m_avro_fingerprint do
  use Bitwise
  def crc64(data) do
    crc64(data, 13933329357911598997)
  end

  defp crc64(<<byte, rest :: binary>>, fp) do
    fp1 = fp >>> 8 ^^^ fp_table((fp ^^^ byte) &&& 255)
    crc64(rest, fp1)
  end

  defp crc64(<<>>, fp) do
    fp
  end

  defp fp_table(0) do
    0
  end

  defp fp_table(1) do
    3238593523956797946
  end

  defp fp_table(2) do
    6477187047913595892
  end

  defp fp_table(3) do
    8435907220062204430
  end

  defp fp_table(4) do
    12954374095827191784
  end

  defp fp_table(5) do
    11472609148414072338
  end

  defp fp_table(6) do
    16871814440124408860
  end

  defp fp_table(7) do
    14327483619285186022
  end

  defp fp_table(8) do
    16515860097293205755
  end

  defp fp_table(9) do
    14539261057490653441
  end

  defp fp_table(10) do
    13607494391182877455
  end

  defp fp_table(11) do
    10387063993012335349
  end

  defp fp_table(12) do
    6265406319754774291
  end

  defp fp_table(13) do
    8791864835633305321
  end

  defp fp_table(14) do
    1085550678754862311
  end

  defp fp_table(15) do
    2585467722461443357
  end

  defp fp_table(16) do
    5247393906202824413
  end

  defp fp_table(17) do
    7215812591205457703
  end

  defp fp_table(18) do
    1239030555549527337
  end

  defp fp_table(19) do
    4449591751341063379
  end

  defp fp_table(20) do
    18092457712352332085
  end

  defp fp_table(21) do
    15556728100436498639
  end

  defp fp_table(22) do
    11742789833002527425
  end

  defp fp_table(23) do
    10234164645493242683
  end

  defp fp_table(24) do
    12530812639509548582
  end

  defp fp_table(25) do
    9302088354573213660
  end

  defp fp_table(26) do
    17583729671266610642
  end

  defp fp_table(27) do
    15633189885995973672
  end

  defp fp_table(28) do
    2171101357509724622
  end

  defp fp_table(29) do
    3661574416647526452
  end

  defp fp_table(30) do
    5170935444922886714
  end

  defp fp_table(31) do
    7724537325157989312
  end

  defp fp_table(32) do
    10494787812405648826
  end

  defp fp_table(33) do
    13642865964979244096
  end

  defp fp_table(34) do
    14431625182410915406
  end

  defp fp_table(35) do
    16480541316673728436
  end

  defp fp_table(36) do
    2478061111099054674
  end

  defp fp_table(37) do
    1049933365183482792
  end

  defp fp_table(38) do
    8899183502682126758
  end

  defp fp_table(39) do
    6300970840149272668
  end

  defp fp_table(40) do
    8399466921467862337
  end

  defp fp_table(41) do
    6368420890995002555
  end

  defp fp_table(42) do
    3275086581351513781
  end

  defp fp_table(43) do
    108854135608684367
  end

  defp fp_table(44) do
    14364169659802000041
  end

  defp fp_table(45) do
    16980263386864569171
  end

  defp fp_table(46) do
    11435870349096892765
  end

  defp fp_table(47) do
    12845837170396948647
  end

  defp fp_table(48) do
    15669858317114364775
  end

  defp fp_table(49) do
    17692196227407282845
  end

  defp fp_table(50) do
    9265331945857609875
  end

  defp fp_table(51) do
    12422293323479818601
  end

  defp fp_table(52) do
    7688114635962061967
  end

  defp fp_table(53) do
    5062151678603773301
  end

  defp fp_table(54) do
    3698085083440658299
  end

  defp fp_table(55) do
    2279937883717887617
  end

  defp fp_table(56) do
    4342202715019449244
  end

  defp fp_table(57) do
    1203395666939462246
  end

  defp fp_table(58) do
    7323148833295052904
  end

  defp fp_table(59) do
    5282940851558637970
  end

  defp fp_table(60) do
    10341870889845773428
  end

  defp fp_table(61) do
    11778178981837571470
  end

  defp fp_table(62) do
    15449074650315978624
  end

  defp fp_table(63) do
    18057156506771531386
  end

  defp fp_table(64) do
    11669866394404287583
  end

  defp fp_table(65) do
    10160817855121008037
  end

  defp fp_table(66) do
    17874829710049597355
  end

  defp fp_table(67) do
    15339802717267265105
  end

  defp fp_table(68) do
    1311848476550706103
  end

  defp fp_table(69) do
    4523114428088083021
  end

  defp fp_table(70) do
    5464845951130112067
  end

  defp fp_table(71) do
    7432843562972398009
  end

  defp fp_table(72) do
    4956122222198109348
  end

  defp fp_table(73) do
    7509300761534850398
  end

  defp fp_table(74) do
    2099866730366965584
  end

  defp fp_table(75) do
    3591042414950500010
  end

  defp fp_table(76) do
    17798367005364253516
  end

  defp fp_table(77) do
    15848531969535615670
  end

  defp fp_table(78) do
    12601941680298545336
  end

  defp fp_table(79) do
    9372796311334617410
  end

  defp fp_table(80) do
    16798933842935724674
  end

  defp fp_table(81) do
    14253900473960229752
  end

  defp fp_table(82) do
    12736841781990005110
  end

  defp fp_table(83) do
    11255500115345754252
  end

  defp fp_table(84) do
    6550173162703027562
  end

  defp fp_table(85) do
    8509314479008689296
  end

  defp fp_table(86) do
    217708271217368734
  end

  defp fp_table(87) do
    3455596968422674276
  end

  defp fp_table(88) do
    870833084869474937
  end

  defp fp_table(89) do
    2370047569572014979
  end

  defp fp_table(90) do
    6194214610827729293
  end

  defp fp_table(91) do
    8721096401170761847
  end

  defp fp_table(92) do
    13822387873690697105
  end

  defp fp_table(93) do
    10602378625989962859
  end

  defp fp_table(94) do
    16587157392570359397
  end

  defp fp_table(95) do
    14609853536892473247
  end

  defp fp_table(96) do
    3483332339477899749
  end

  defp fp_table(97) do
    2064482512161650719
  end

  defp fp_table(98) do
    7616958077116566033
  end

  defp fp_table(99) do
    4991418462803860459
  end

  defp fp_table(100) do
    9480190278288059917
  end

  defp fp_table(101) do
    12637572737790640119
  end

  defp fp_table(102) do
    15741190762473065977
  end

  defp fp_table(103) do
    17762823925471730691
  end

  defp fp_table(104) do
    15376229271924123934
  end

  defp fp_table(105) do
    17983608511393921252
  end

  defp fp_table(106) do
    10124303357207546602
  end

  defp fp_table(107) do
    11561034798826117904
  end

  defp fp_table(108) do
    7396170166881316598
  end

  defp fp_table(109) do
    5356383260452470540
  end

  defp fp_table(110) do
    4559875767435775234
  end

  defp fp_table(111) do
    1420363961462201592
  end

  defp fp_table(112) do
    8684405430038898488
  end

  defp fp_table(113) do
    6085769495188764354
  end

  defp fp_table(114) do
    2406791333878924492
  end

  defp fp_table(115) do
    979366144819647798
  end

  defp fp_table(116) do
    14646297666590105808
  end

  defp fp_table(117) do
    16695918618875998506
  end

  defp fp_table(118) do
    10565881703117275940
  end

  defp fp_table(119) do
    13713538703073841886
  end

  defp fp_table(120) do
    11362911691697612739
  end

  defp fp_table(121) do
    12772455230081578553
  end

  defp fp_table(122) do
    14146576876296094775
  end

  defp fp_table(123) do
    16763373153642681805
  end

  defp fp_table(124) do
    3347869283551649835
  end

  defp fp_table(125) do
    182341662412566993
  end

  defp fp_table(126) do
    8616954185191982047
  end

  defp fp_table(127) do
    6585487012709290533
  end

  defp fp_table(128) do
    13933329357911598997
  end

  defp fp_table(129) do
    17126321439046432367
  end

  defp fp_table(130) do
    11006435164953838689
  end

  defp fp_table(131) do
    12992741788688209307
  end

  defp fp_table(132) do
    8257930048646602877
  end

  defp fp_table(133) do
    6803747195591438727
  end

  defp fp_table(134) do
    3132703159877387145
  end

  defp fp_table(135) do
    542775339377431155
  end

  defp fp_table(136) do
    2623696953101412206
  end

  defp fp_table(137) do
    619515277774763668
  end

  defp fp_table(138) do
    9046228856176166042
  end

  defp fp_table(139) do
    5871394916501263712
  end

  defp fp_table(140) do
    10929691902260224134
  end

  defp fp_table(141) do
    13501751302614184316
  end

  defp fp_table(142) do
    14865687125944796018
  end

  defp fp_table(143) do
    16338017159720129160
  end

  defp fp_table(144) do
    9912244444396218696
  end

  defp fp_table(145) do
    11925134239902742706
  end

  defp fp_table(146) do
    15018601523069700796
  end

  defp fp_table(147) do
    18202706530865158982
  end

  defp fp_table(148) do
    4199733460733931168
  end

  defp fp_table(149) do
    1637543290675756890
  end

  defp fp_table(150) do
    7182084829901000020
  end

  defp fp_table(151) do
    5717935174548446382
  end

  defp fp_table(152) do
    7834929158557182387
  end

  defp fp_table(153) do
    4632665972928804937
  end

  defp fp_table(154) do
    3844057317981030983
  end

  defp fp_table(155) do
    1849042541720329149
  end

  defp fp_table(156) do
    16103865201353027163
  end

  defp fp_table(157) do
    17549867708331900833
  end

  defp fp_table(158) do
    9700748483321744815
  end

  defp fp_table(159) do
    12280807109898935381
  end

  defp fp_table(160) do
    5834933197202143791
  end

  defp fp_table(161) do
    8937414855024798677
  end

  defp fp_table(162) do
    655924238275353051
  end

  defp fp_table(163) do
    2732422975565056033
  end

  defp fp_table(164) do
    16374796089197559239
  end

  defp fp_table(165) do
    14974255385173568573
  end

  defp fp_table(166) do
    13465025131935292979
  end

  defp fp_table(167) do
    10821211621719183305
  end

  defp fp_table(168) do
    13100346325406055124
  end

  defp fp_table(169) do
    11041713811386575662
  end

  defp fp_table(170) do
    17018628958017378592
  end

  defp fp_table(171) do
    13897997918303815898
  end

  defp fp_table(172) do
    435416542434737468
  end

  defp fp_table(173) do
    3097107305413864646
  end

  defp fp_table(174) do
    6911193936845348552
  end

  defp fp_table(175) do
    8293578696285179698
  end

  defp fp_table(176) do
    1741666169738949874
  end

  defp fp_table(177) do
    3808479038558283016
  end

  defp fp_table(178) do
    4740095139144029958
  end

  defp fp_table(179) do
    7870595381236532988
  end

  defp fp_table(180) do
    12388429221655458586
  end

  defp fp_table(181) do
    9736009554713699040
  end

  defp fp_table(182) do
    17442192802341523694
  end

  defp fp_table(183) do
    16068516186704462100
  end

  defp fp_table(184) do
    18239503069743100937
  end

  defp fp_table(185) do
    15127152172900050419
  end

  defp fp_table(186) do
    11888425678624364541
  end

  defp fp_table(187) do
    9803746554456753671
  end

  defp fp_table(188) do
    5681455845848806369
  end

  defp fp_table(189) do
    7073288438148047387
  end

  defp fp_table(190) do
    1673934641775824917
  end

  defp fp_table(191) do
    4308477092595991023
  end

  defp fp_table(192) do
    6966664678955799498
  end

  defp fp_table(193) do
    5503217582476919344
  end

  defp fp_table(194) do
    4128965024323301438
  end

  defp fp_table(195) do
    1566351579938693572
  end

  defp fp_table(196) do
    15233916154233132066
  end

  defp fp_table(197) do
    18417600011429070296
  end

  defp fp_table(198) do
    9982836925607720918
  end

  defp fp_table(199) do
    11996431537128302124
  end

  defp fp_table(200) do
    9627165335515697969
  end

  defp fp_table(201) do
    12207926510359495371
  end

  defp fp_table(202) do
    15886756170769674437
  end

  defp fp_table(203) do
    17332335396841578815
  end

  defp fp_table(204) do
    3917464579278591193
  end

  defp fp_table(205) do
    1922028658990515491
  end

  defp fp_table(206) do
    8051932600676513581
  end

  defp fp_table(207) do
    4850374241660872407
  end

  defp fp_table(208) do
    2917466598601071895
  end

  defp fp_table(209) do
    327962119137676525
  end

  defp fp_table(210) do
    8187398044598779619
  end

  defp fp_table(211) do
    6732512565967646489
  end

  defp fp_table(212) do
    11221777246008269567
  end

  defp fp_table(213) do
    13207379120439233285
  end

  defp fp_table(214) do
    14004037317153847563
  end

  defp fp_table(215) do
    17197450482186430705
  end

  defp fp_table(216) do
    14792340333762633196
  end

  defp fp_table(217) do
    16265093719173729302
  end

  defp fp_table(218) do
    10712766520904941080
  end

  defp fp_table(219) do
    13284123302255603682
  end

  defp fp_table(220) do
    9119751534871550468
  end

  defp fp_table(221) do
    5944212839312182270
  end

  defp fp_table(222) do
    2840727922924403184
  end

  defp fp_table(223) do
    836967320887912458
  end

  defp fp_table(224) do
    17368810860077796976
  end

  defp fp_table(225) do
    15995557527495450506
  end

  defp fp_table(226) do
    12171538990377528708
  end

  defp fp_table(227) do
    9518416773021940862
  end

  defp fp_table(228) do
    4813582667757848984
  end

  defp fp_table(229) do
    7943378085384837218
  end

  defp fp_table(230) do
    1958732289639295596
  end

  defp fp_table(231) do
    4025966300338256790
  end

  defp fp_table(232) do
    1458733299300535947
  end

  defp fp_table(233) do
    4093699022299389809
  end

  defp fp_table(234) do
    5610888623004134783
  end

  defp fp_table(235) do
    7002018658576923781
  end

  defp fp_table(236) do
    12103802978479819107
  end

  defp fp_table(237) do
    10018419036150929561
  end

  defp fp_table(238) do
    18310175810188503703
  end

  defp fp_table(239) do
    15198246066092718957
  end

  defp fp_table(240) do
    13391477134206599341
  end

  defp fp_table(241) do
    10748366240846565719
  end

  defp fp_table(242) do
    16157651908532642649
  end

  defp fp_table(243) do
    14756687855020634787
  end

  defp fp_table(244) do
    729366649650267973
  end

  defp fp_table(245) do
    2805444311502067391
  end

  defp fp_table(246) do
    6051901489239909553
  end

  defp fp_table(247) do
    9155087905094251851
  end

  defp fp_table(248) do
    6695738567103299670
  end

  defp fp_table(249) do
    8078825954266321324
  end

  defp fp_table(250) do
    364683324825133986
  end

  defp fp_table(251) do
    3025950744619954776
  end

  defp fp_table(252) do
    17233908370383964094
  end

  defp fp_table(253) do
    14112856248920397380
  end

  defp fp_table(254) do
    13170974025418581066
  end

  defp fp_table(255) do
    11113046258555286960
  end

end