#!/usr/bin/env bun
// @bun
import{join as E}from"path";import{Database as S}from"bun:sqlite";import{mkdirSync as g,writeFileSync as w,existsSync as n}from"fs";var D=process.cwd(),A=(X,K)=>{if(!X)return K;let L=Number.parseInt(X,10);return Number.isFinite(L)&&L>0?L:K},q={dbPath:process.env.DB_PATH??E(D,"data","eshop_kr.sqlite"),sqliteBusyTimeoutMs:A(process.env.SQLITE_BUSY_TIMEOUT_MS,30000),userAgent:process.env.USER_AGENT??"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",requestTimeoutMs:A(process.env.REQUEST_TIMEOUT_MS,30000),priceApiEndpoint:process.env.PRICE_API_ENDPOINT??"https://api.ec.nintendo.com/v1/price",priceApiBatchSize:Math.min(A(process.env.PRICE_API_BATCH_SIZE,50),50),priceApiThrottleMs:A(process.env.PRICE_API_THROTTLE_MS,100),priceApiCountry:process.env.PRICE_API_COUNTRY??"KR",priceApiLang:process.env.PRICE_API_LANG??"ko"},H=null,p=(X)=>{if(H)return H;let K=X?.path??q.dbPath;return H=new S(K,{create:!X?.readonly,readonly:X?.readonly}),H.exec(`PRAGMA busy_timeout = ${q.sqliteBusyTimeoutMs}`),H.exec("PRAGMA journal_mode = WAL"),H.exec("PRAGMA foreign_keys = ON"),H},l=()=>{H?.close(),H=null},h=(X)=>{return X.query(`SELECT id, store_code as storeCode FROM products
       WHERE is_active = 1 AND store_code GLOB '700*'
       ORDER BY id`).all()},C=(X,K)=>{let L=K==="US"?"nsuid_us":"nsuid_jp";return X.query(`SELECT id, ${L} as storeCode, '${K}' as country FROM products
       WHERE is_active = 1 AND ${L} IS NOT NULL
       ORDER BY id`).all()},d=(X,K)=>{if(X==null||K==null||X<=0)return null;if(K>=X)return 0;return Number(((1-K/X)*100).toFixed(2))},u=(X,K)=>X!=null&&K!=null&&X>0&&K<X?1:0,c=(X,K)=>{let L=u(K.priceRegular,K.priceFinal),O=d(K.priceRegular,K.priceFinal),$=K.saleStartAt??null,J=K.saleEndAt??null,V=K.country??"KR",Y=X.query(`SELECT id, price_regular, price_final, on_sale, sale_start_at, sale_end_at
       FROM price_ranges WHERE product_id = ? AND country = ? AND ended_at IS NULL
       ORDER BY id DESC LIMIT 1`).get(K.productId,V);if(Y){if(Y.price_regular===K.priceRegular&&Y.price_final===K.priceFinal&&Y.on_sale===L&&(Y.sale_start_at??"")===($ ??"" )&&(Y.sale_end_at??"")===(J??""))return!1;X.query("UPDATE price_ranges SET ended_at = datetime('now') WHERE id = ?").run(Y.id)}return X.query(`INSERT INTO price_ranges (product_id, country, price_regular, price_final, discount_rate, on_sale, sale_start_at, sale_end_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(K.productId,V,K.priceRegular,K.priceFinal,O,L,$,J),!0},i=async(X,K)=>{if(X.length===0)return new Map;if(X.length>50)throw Error(`Batch size ${X.length} exceeds API limit of 50`);let L=K?.country??q.priceApiCountry,O=K?.lang??q.priceApiLang,$=`${q.priceApiEndpoint}?country=${L}&lang=${O}&ids=${X.join(",")}`,J=await fetch($,{headers:{"user-agent":q.userAgent,accept:"application/json","accept-language":"ko-KR,ko;q=0.9"},signal:AbortSignal.timeout(q.requestTimeoutMs)});if(!J.ok)throw Error(`Price API HTTP ${J.status}`);let V=await J.json(),Y=new Map,U=(Z)=>{let G=Number.parseFloat(Z);return Z.includes(".")?Math.round(G*100):G};for(let Z of V.prices??[]){let G=String(Z.title_id),v=Z.regular_price?U(Z.regular_price.raw_value):null,B=Z.discount_price?U(Z.discount_price.raw_value):null;Y.set(G,{storeCode:G,priceRegular:v,priceFinal:B??v,saleStartAt:Z.discount_price?.start_datetime??null,saleEndAt:Z.discount_price?.end_datetime??null,salesStatus:Z.sales_status})}return Y},a=async(X,K,L)=>{let O=new Map,$=q.priceApiBatchSize,J=Math.ceil(X.length/$);for(let V=0;V<J;V++){let Y=X.slice(V*$,(V+1)*$),U=await i(Y,L);for(let[Z,G]of U)O.set(Z,G);if(K?.(V+1,J,U.size),V<J-1)await Bun.sleep(q.priceApiThrottleMs)}return O},y={KR:"ko",US:"en",JP:"ja"};async function s(X){let K=X.includes("--all"),L=X.find((Z)=>Z.startsWith("--country="))?.split("=")[1]?.toUpperCase(),O=p(),$=[];if(K){$.push({country:"KR",lang:"ko",products:h(O)});for(let Z of["US","JP"]){let G=C(O,Z);if(G.length>0)$.push({country:Z,lang:y[Z],products:G})}}else if(L&&L!=="KR"){let Z=L,G=C(O,Z);$.push({country:Z,lang:y[Z]??"en",products:G})}else $.push({country:"KR",lang:"ko",products:h(O)});let J=Date.now(),V=0,Y=0,U=0;try{for(let Z of $){let{country:G,lang:v,products:B}=Z;if(B.length===0){console.log(`Price sync [${G}]  0 products - skipping`);continue}let j=Date.now(),P=new Map,x=[];for(let z of B)P.set(z.storeCode,z.id),x.push(z.storeCode);let k=O.query("INSERT INTO price_sync_log (total_products) VALUES (0)").run(),F=Number(k.lastInsertRowid);console.log(`Price sync [${G}]  ${x.length} products`);let m=0,Q=0,M=0,W=await a(x,(z,N,R)=>{if(m++,z%20===0||z===N){let f=((Date.now()-j)/1000).toFixed(1);console.log(`  [${f}s]  batch ${z}/${N}  prices=${R}  changed=${Q}`)}},{country:G,lang:v});for(let[z,N]of W){let R=P.get(z);if(!R)continue;if(N.salesStatus==="not_found")continue;try{if(c(O,{productId:R,priceRegular:N.priceRegular,priceFinal:N.priceFinal,saleStartAt:N.saleStartAt,saleEndAt:N.saleEndAt,country:G}))Q++}catch{M++}}let T=Date.now()-j;O.query(`UPDATE price_sync_log SET
           ended_at = datetime('now'), status = ?, total_products = ?,
           api_requests = ?, prices_changed = ?, errors = ?, duration_ms = ?,
           notes = ?
         WHERE id = ?`).run(M>0?"partial":"success",x.length,m,Q,M,T,`country=${G}`,F),console.log(`[${G}] sync complete [${(T/1000).toFixed(1)}s]  products=${x.length}  requests=${m}  changed=${Q}  errors=${M}`),V+=Q,Y+=M,U+=x.length}if($.length>1){let Z=Date.now()-J;console.log(`\nAll price syncs complete [${(Z/1000).toFixed(1)}s]  countries=${$.map((G)=>G.country).join(",")}  products=${U}  changed=${V}  errors=${Y}`)}}catch(Z){console.error("Price sync failed:",Z),process.exit(1)}finally{l()}}async function o(){let X=E(D,"dist"),K=E(X,"data"),L=q.dbPath,O=4194304;g(K,{recursive:!0});let $=n(L)?new S(L,{readonly:!0}):null;if(!$)console.error("Database not found:",L),process.exit(1);let J=(()=>{try{return $.query("PRAGMA table_info(price_ranges)").all().some((M)=>M.name==="country")}catch{return!1}})(),V=J?"AND pr.country = 'KR'":"",Y=(Q="")=>`
    SELECT
      p.id as productId, p.canonical_url as canonicalUrl, p.title, p.publisher,
      p.release_date as releaseDate, p.product_type as productType,
      pr.price_regular as priceRegular, pr.price_final as priceFinal,
      pr.discount_rate as discountRate, pr.sale_start_at as saleStartAt,
      pr.sale_end_at as saleEndAt, pr.started_at as lastCapturedAt,
      d.genre, d.languages, d.platform_labels as platformLabels, d.thumbnail_url as thumbnailUrl
    FROM products p
    INNER JOIN price_ranges pr ON p.id = pr.product_id AND pr.ended_at IS NULL ${V}
    LEFT JOIN product_details d ON p.id = d.product_id
    WHERE pr.on_sale = 1 AND pr.sale_end_at > datetime('now') ${Q}
  `;function U(Q){let M=J?", p.deku_slug as dekuSlug":"",W=$.query(`
      SELECT p.id as productId, p.canonical_url as canonicalUrl, p.title, p.publisher,
        p.release_date as releaseDate, p.product_type as productType,
        pr.price_regular as priceRegular, pr.price_final as priceFinal,
        pr.discount_rate as discountRate, pr.sale_start_at as saleStartAt,
        pr.sale_end_at as saleEndAt, pr.started_at as lastCapturedAt,
        d.genre, d.languages, d.platform_labels as platformLabels,
        d.description_html as description, d.thumbnail_url as thumbnailUrl,
        d.gallery_urls_json, d.attribute_rows_json,
        p.meta_genre_ids as metaGenreIds, p.meta_language_ids as metaLanguageIds,
        p.meta_platform_ids as metaPlatformIds, p.meta_publisher_id as metaPublisherId
        ${M}
      FROM products p
      LEFT JOIN price_ranges pr ON p.id = pr.product_id AND pr.ended_at IS NULL ${V}
      LEFT JOIN product_details d ON p.id = d.product_id
      WHERE p.id = ?
    `).get(Q);if(!W)return null;let T=W.gallery_urls_json?JSON.parse(W.gallery_urls_json):[],z=W.attribute_rows_json?JSON.parse(W.attribute_rows_json):[],N=J?"AND country = 'KR'":"",R=$.query(`
      SELECT started_at, ended_at, price_regular, price_final, discount_rate, on_sale
      FROM price_ranges WHERE product_id = ? AND price_final IS NOT NULL ${N}
      ORDER BY started_at ASC
    `).all(Q),f=[],_=[];if(J)f=$.query(`
        SELECT started_at, ended_at, price_regular, price_final, discount_rate, on_sale
        FROM price_ranges WHERE product_id = ? AND country = 'US' AND price_final IS NOT NULL
        ORDER BY started_at ASC
      `).all(Q),_=$.query(`
        SELECT started_at, ended_at, price_regular, price_final, discount_rate, on_sale
        FROM price_ranges WHERE product_id = ? AND country = 'JP' AND price_final IS NOT NULL
        ORDER BY started_at ASC
      `).all(Q);let I={...W,galleryUrls:T,attributeRows:z,priceHistory:R};if(delete I.gallery_urls_json,delete I.attribute_rows_json,f.length>0)I.priceHistoryUs=f;if(_.length>0)I.priceHistoryJp=_;return I}function Z(Q){return`${Q.getFullYear()}-${String(Q.getMonth()+1).padStart(2,"0")}-${String(Q.getDate()).padStart(2,"0")}`}function G(Q){let M=new Set;return Q.forEach((W)=>{if(W.saleEndAt)M.add(Z(new Date(W.saleEndAt)))}),Array.from(M).sort()}console.log("Exporting web data...");let v=$.query(Y()+" ORDER BY pr.discount_rate DESC NULLS LAST, p.title ASC").all(),B=$.query(Y("AND pr.sale_end_at <= datetime('now', '+7 days')")+" ORDER BY pr.sale_end_at ASC").all(),j=$.query(`
    SELECT
      p.id as productId, p.canonical_url as canonicalUrl, p.title, p.publisher,
      p.release_date as releaseDate, p.product_type as productType,
      COALESCE(pr.price_regular, 0) as priceRegular,
      COALESCE(pr.price_final, pr.price_regular, 0) as priceFinal,
      COALESCE(pr.discount_rate, 0) as discountRate,
      pr.on_sale as onSale,
      pr.sale_start_at as saleStartAt, pr.sale_end_at as saleEndAt,
      pr.started_at as lastCapturedAt,
      d.genre, d.languages, d.platform_labels as platformLabels, d.thumbnail_url as thumbnailUrl
    FROM products p
    LEFT JOIN price_ranges pr ON p.id = pr.product_id AND pr.ended_at IS NULL ${V}
    LEFT JOIN product_details d ON p.id = d.product_id
    WHERE p.is_active = 1
    ORDER BY p.title ASC
  `).all(),P={},x={},k=0,F=0;function m(){if(Object.keys(x).length===0)return;w(E(K,`details-${F}.json`),JSON.stringify(x)),console.log(`  details-${F}.json: ${Object.keys(x).length} products (~${(k/1024/1024).toFixed(1)}MB)`),x={},k=0,F++}for(let Q of j){let M=U(Q.productId);if(!M)continue;let T=JSON.stringify(M).length+10;if(k+T>4194304&&Object.keys(x).length>0)m();x[Q.productId]=M,k+=T,P[Q.productId]=F}m(),w(E(K,"details-index.json"),JSON.stringify(P)),console.log(`  details-index.json: ${Object.keys(P).length} entries -> ${F} chunks`),w(E(K,"current-sales.json"),JSON.stringify({generatedAt:new Date().toISOString(),count:v.length,items:v,endDates:G(v)})),w(E(K,"ending-soon.json"),JSON.stringify({generatedAt:new Date().toISOString(),count:B.length,items:B,endDates:G(B)})),w(E(K,"all-products.json"),JSON.stringify({generatedAt:new Date().toISOString(),count:j.length,items:j}));try{if($.query("SELECT name FROM sqlite_master WHERE type='table' AND name='store_meta_filters'").get()){let M=$.query("SELECT filter_key, filter_name, query_param, multi_select FROM store_meta_filters ORDER BY sort_order").all(),W=$.query("SELECT filter_key, option_id, label, product_count FROM store_meta_options ORDER BY filter_key, sort_order").all();w(E(K,"meta-filters.json"),JSON.stringify({generatedAt:new Date().toISOString(),filters:M,options:W})),console.log(`  meta-filters.json: ${M.length} filters`)}}catch{}console.log(`  current-sales.json: ${v.length} items`),console.log(`  ending-soon.json: ${B.length} items`),console.log(`  all-products.json: ${j.length} items`),console.log("Export complete!"),$.close()}async function e(){let X=E(D,"dist"),K=E(X,"data"),L=new S(q.dbPath,{readonly:!0});L.exec(`PRAGMA busy_timeout = ${q.sqliteBusyTimeoutMs}`);let O=L.query(`
    SELECT
      COUNT(*) as total,
      SUM(is_active) as active,
      SUM(CASE WHEN detail_fetched_at IS NOT NULL THEN 1 ELSE 0 END) as withDetail,
      (SELECT COUNT(DISTINCT product_id) FROM price_ranges) as withPrice,
      (SELECT COUNT(DISTINCT product_id) FROM price_ranges WHERE ended_at IS NULL AND on_sale = 1) as onSale
    FROM products
  `).get(),$=L.query("SELECT product_type, COUNT(*) as cnt FROM products GROUP BY product_type ORDER BY cnt DESC").all(),J=L.query("SELECT status, COUNT(*) as cnt FROM detail_queue GROUP BY status").all(),V={pending:0,processing:0,done:0,failed:0};for(let G of J)if(G.status in V)V[G.status]=G.cnt;let Y=L.query(`
    SELECT started_at, ended_at, status, total_products, api_requests, prices_changed, errors, duration_ms
    FROM price_sync_log ORDER BY id DESC LIMIT 1
  `).get(),U=L.query(`
    SELECT id, run_type, started_at, ended_at, status, items_processed, items_changed, errors
    FROM crawl_runs ORDER BY id DESC LIMIT 10
  `).all();L.close();let Z={generatedAt:new Date().toISOString(),products:{...O,byType:Object.fromEntries($.map((G)=>[G.product_type,G.cnt]))},prices:{currentOnSale:O.onSale,lastSync:Y},detailQueue:V,recentRuns:U};g(K,{recursive:!0}),w(E(K,"status.json"),JSON.stringify(Z)),console.log("Status exported to dist/data/status.json")}var b=process.argv[2],r=process.argv.slice(3);switch(b){case"sync-prices":await s(r);break;case"export-data":await o();break;case"status":await e();break;default:console.error(`Unknown command: ${b}`),console.error("Usage: runner.mjs <sync-prices|export-data|status> [options]"),process.exit(1)}
