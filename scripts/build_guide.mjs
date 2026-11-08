// Rebuild: npm install marked; node scripts/build_guide.mjs
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const root=path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const {marked}=await import(process.env.MARKED_MODULE || 'marked');
let html=marked.parse(fs.readFileSync(path.join(root,'docs/guide.md'),'utf8'));
let index=0;const toc=[];
html=html.replace(/<h([123])>(.*?)<\/h\1>/gs,(_,level,title)=>{
 const id='section-'+(++index);
 if(level==='2')toc.push(`<a href="#${id}">${title}</a>`);
 return `<h${level} id="${id}">${title}</h${level}>`;
});
html=html.replace(/src="(images\/[^\"]+)"/g,(_,name)=>{
 const ext=path.extname(name);const mime=ext==='.svg'?'image/svg+xml':'image/png';
 return `src="data:${mime};base64,${fs.readFileSync(path.join(root,'docs',name)).toString('base64')}"`;
});
const css=`*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:#f4f6f9;color:#1d2736;font:17px/1.65 system-ui,-apple-system,sans-serif}nav{position:fixed;left:0;top:0;width:270px;height:100vh;overflow:auto;padding:26px 20px;background:#16253b;color:white}nav strong{display:block;margin-bottom:18px}nav a{display:block;color:#d8e4f6;text-decoration:none;font-size:13px;line-height:1.5;padding:6px 0}main{max-width:1100px;margin-left:270px;padding:48px 60px 90px;background:white}h1{font-size:38px;line-height:1.2;margin-top:0}h2{font-size:27px;line-height:1.3;margin-top:60px;scroll-margin-top:20px;border-top:1px solid #dbe2eb;padding-top:24px}h3{font-size:22px;margin-top:35px}p{margin:16px 0}a{color:#225f9f}pre{background:#142339;color:#e7eef8;padding:22px;overflow:auto;border-radius:7px;font-size:13px;line-height:1.6}code{font-family:ui-monospace,Menlo,monospace;font-size:.88em}p code,li code,td code{background:#edf2f7;padding:2px 4px;overflow-wrap:anywhere}table{border-collapse:collapse;width:100%;font-size:14px;line-height:1.5;margin:24px 0}th,td{border:1px solid #d5dfeb;padding:12px;text-align:left;vertical-align:top}th{background:#e7edf6}tr:nth-child(even){background:#f7f9fc}img{width:100%;height:auto;display:block;margin:28px 0}li{margin:7px 0}button{border:0;padding:10px 18px;background:#2a6bb1;color:white;border-radius:5px;cursor:pointer}.meta{color:#5d6c80;font-size:14px;margin-bottom:28px}@media(max-width:1000px){nav{position:static;width:auto;height:auto;max-height:250px}main{margin:0;padding:28px 20px}table{display:block;overflow:auto}h1{font-size:30px}}@media print{nav,button{display:none}main{margin:0;max-width:none;padding:0;font-size:11pt}h2{break-before:page}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f1f3f5;color:black;font-size:8pt}table{font-size:9pt}img,pre{break-inside:avoid}a{color:inherit}}`;
fs.writeFileSync(path.join(root,'docs/GUIDE.html'),`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Video Harvester — руководство для Никиты</title><style>${css}</style><body><nav><strong>Video Harvester<br>Руководство для Никиты</strong>${toc.join('')}</nav><main><div class="meta">Лабораторная работа 1 · версия протокола 2.0 · 8 ноября 2026</div><button onclick="window.print()">Печать или PDF</button>${html}</main></body></html>`);
console.log('Offline guide built');
