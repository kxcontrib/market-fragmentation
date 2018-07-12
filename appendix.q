///////////////////////////////////////////////////////////////////////////////////////
// Script to accompany Technical Whitepaper
// - Market Fragmentation: A kdb+ framework for multiple liquidity sources (Jan 2013)
//
// Author: James Corcoran (jcorcoran@kx.com)
///////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////
// Set up configuration data
////////////////////////////////

.cfg.filterrules:()!();
 .cfg.filterrules[`TM]:([venue:`LSE`BAT`CHI`TOR]
                       qualifier:(
                          `A`Auc`B`C`X`DARKTRADE`m;
                          `A`AUC`B`c`x`D ARK;
                          `a`auc`b`c`x`DRK;
                          `A`Auc`B`C`X`DARKTRADE`m)
                        );
.cfg.filterrules[`OB]:([venue:`LSE`BAT`CHI`TOR]
                       qualifier:(`A`Auc`B`C`m;`A`AUC`B`c;`a`auc`b`c;`A`A uc`B`C`m));
.cfg.filterrules[`DRK]:([venue:`LSE`BAT`CHI`TOR]
                         qualifier:`DARKTRADE`DARK`DRK`DARKTRADE);

.cfg.symVenue:()!();
.cfg.symVenue[`BARCl.BS]:`BAT;
.cfg.symVenue[`BARCl.CHI]:`CHI;
.cfg.symVenue[`BARC.L]:`LSE;
.cfg.symVenue[`BARC.TQ]:`TOR;
.cfg.symVenue[`VODl.BS]:`BAT;
.cfg.symVenue[`VODl.CHI]:`CHI;
.cfg.symVenue[`VOD.L]:`LSE;
.cfg.symVenue[`VODl.TQ]:`TOR;

.cfg.multiMarketMap:([sym:`BARCl.BS`BARCl.CHI`BARC.L`BARC.TQ`VODl.BS`VODl.CHI`VOD.L`VODl.TQ] 
                     primarysym:`BARC.L`BARC.L`BARC.L`BARC.L`VOD.L`VOD.L`VOD.L`VOD.L;
                     venue:`BAT`CHI`LSE`TOR`BAT`CHI`LSE`TOR);

.cfg.multiMarketAgg:()!();
.cfg.multiMarketAgg[`volume]:"sum volume"
.cfg.multiMarketAgg[`vwap]:"wavg[volume;vwap]"
.cfg.multiMarketAgg[`range]: "(max maxprice)-(min minprice)"
.cfg.multiMarketAgg[`tickcount]:"sum tickcount"
.cfg.multiMarketAgg[`maxbid]:"max maxbid"
.cfg.multiMarketAgg[`minask]:"min minask"
.cfg.multiMarketAgg[`lastmidprice]:"((max lastbid)+(min lastask))%2"

.cfg.defaultParams:`startTime`endTime`filterRule`multiMarketRule!(08:30;16:30;`OB;`none);

////////////////////////////////
// Analytic functions
////////////////////////////////

getIntervalData:{[params]
    -1"Running getIntervalData for params: ",-3!params;
    params:.util.applyDefaultParams[params]; 
    if[params[`multiMarketRule]~`multi;
        extended_syms:.util.extendSymsForMultiMarket[params`symList]; 
        params:@[params;`symList;:;extended\_syms`symList];
    ];

res:select volume:sum[size], vwap:wavg[size;price], range:max[price]-min[price], 
           maxprice:max price, minprice:min price,
           maxbid:max bid, minask:min ask,
           lastbid:last bid, lastask:last ask, lastmidprice:(last[bid]+last[ask])%2 
    by sym from trade
    where date=params[`date],
          sym in params[`symList],
          time within (params`startTime;params`endTime),
          .util.validTrade[sym;qualifier;params`filterRule];

if[params[`multiMarketRule]~`multi;
    res:lj[res;`sym xkey select sym:symList, origSymList from extended_syms]; 
    byClause:(enlist`sym)!enlist`origSymList;
    aggClause:columns!-5!'.cfg.multiMarketAgg[columns:params`columns]; 
    res:0!?[res;();byClause;aggClause];
  ];
  :(`sym,params[`columns])\#0!res
};

////////////////////////////////
// Utilities
////////////////////////////////

.util.applyDefaultParams:{[params]
    .cfg.defaultParams,params
    };

.util.validTrade:{[sym;qualifier;rule] 
    venue:.cfg.symVenue[sym];
    validqualifiers:(.cfg.filterrules[rule]each venue)`qualifier; 
    first each qualifier in' validqualifiers
    };

.util.extendSymsForMultiMarket:{[symList] 
    distinct raze {update origSymList:x from
                   select symList:sym from .cfg.multiMarketMap
                   where primarysym in .cfg.multiMarketMap[x]`primarysym
                   } each (),symList
    }

////////////////////////////////
// Generate trade data
////////////////////////////////
\P 6

trade:([]date:`date$();sym:`$();time:`time$();price:`float$();size:`int$());

pi:acos -1;
/ Box-muller from kx.com/q/stat.q
nor:{$[x=2*n:x div 2;raze sqrt[-2*log n?1f]*/:(sin;cos)@\:(2\*pi)\*n?1f;-1_.z.s 1+x]} 

generateRandomPrices:{[s0;n] 
    dt:1%365*1000;
    timesteps:n; 
    vol:.2; 
    mu:.01;
    randomnumbers:sums(timesteps;1)#(nor timesteps); 
    s:s0\*exp[(dt*mu-xexp[vol;2]%2) + randomnumbers*vol*sqrt[dt]]; 
    raze s}

n:1000;
`trade insert (n#2013.01.15;
               n?`BARCl.BS`BARCl.CHI`BARC.L`BARC.TQ; 
               08:00:00.000+28800*til n;
               generateRandomPrices[244;n]; 10*n?100000);

`trade insert (n#2013.01.15;
               n?`VODl.BS`VODl.CHI`VOD.L`VODl.TQ; 
               08:00:00.000+28800*til n;
               generateRandomPrices[161;n]; 
               10*n?100000);

/ add dummy qualifiers
trade:{update qualifier:1?.cfg.filterrules[`TM;.cfg.symVenue[sym]]`qualifier from x}each trade;

/ add dummy prevailing quotes 
spread:0.01;
update bid:price-0.5*spread, ask:price+0.5*spread from `trade;

`time xasc `trade;

////////////////////////////////
// Usage
////////////////////////////////

params:`symList`date`startTime`endTime`columns!(
    `VOD.L`BARC.L; 
    2013.01.15;
    08:30;09:30;
    `volume`vwap`range`maxbid`minask`lastmidprice);

/ default, filterRule=orderbook & multiMarketRule=none 
a:getIntervalData params;

/ change filterRule from 'orderbook' to 'total market' 
b:getIntervalData @[params;`filterRule;:;`TM];

/ change multiMarketRule from 'none' to 'multi' to get consolidated analytics 
c:getIntervalData @[params;`multiMarketRule;:;`multi];
