// Assembles the site's content from the repository's Markdown. Runs from
// astro.config.mjs before every build and dev server, and again in dev when a
// source file changes; `npm run content` runs it alone. It writes
// src/content/docs/ for the pages, and public/ for the images, a plain .md
// copy of every page, llms.txt (an index for agents) and llms-full.txt (every
// page in one file). Links are rewritten so they work in each place: between
// pages on the site, between .md copies for agents, and to GitHub for any
// other file in the repository.
import { cpSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, posix } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { base, groups, pages, repo, site } from './pages.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '../..');
const content = join(here, 'src/content/docs');
const pub = join(here, 'public');

const bySrc = new Map(pages.map((p) => [p.src, p]));
const mdName = (p) => `${p.md ?? p.slug}.md`;
const pageUrl = (p) => (p.slug === 'index' ? `${base}/` : `${base}/${p.slug}/`);

// Rewrites the targets of Markdown links and images, outside code blocks.
function rewrite(text, src, to) {
	let fenced = false;
	return text
		.split('\n')
		.map((line) => {
			if (/^\s*```/.test(line)) fenced = !fenced;
			if (fenced) return line;
			return line.replace(/(!?\[[^\]]*\])\(([^)\s]+)\)/g, (all, label, target) => {
				if (/^[a-z]+:/i.test(target) || target.startsWith('#')) return all;
				const [path, anchor] = target.split('#');
				const resolved = posix.normalize(posix.join(posix.dirname(src), path));
				return `${label}(${to(resolved, anchor ? `#${anchor}` : '')})`;
			});
		})
		.join('\n');
}

function forSite(resolved, anchor) {
	const p = bySrc.get(resolved);
	if (p) return pageUrl(p) + anchor;
	if (resolved.startsWith('docs/images/')) return `${base}/images/${resolved.slice('docs/images/'.length)}`;
	return `${repo}/blob/main/${resolved}${anchor}`;
}

function forAgents(resolved, anchor) {
	const p = bySrc.get(resolved);
	if (p) return `${site}${base}/${mdName(p)}${anchor}`;
	if (resolved.startsWith('docs/images/')) return `${site}${base}/images/${resolved.slice('docs/images/'.length)}`;
	return `${repo}/blob/main/${resolved}${anchor}`;
}

// Drops the page's own "# Title" line: Starlight shows the title itself.
const withoutTitle = (text) => text.replace(/^\s*# .*\n+/, '');

// The source files, for the dev server to watch.
export const sources = pages.map((p) => join(root, p.src));

export function assemble() {
	mkdirSync(content, { recursive: true });
	mkdirSync(pub, { recursive: true });
	rmSync(join(pub, 'images'), { recursive: true, force: true });
	cpSync(join(root, 'docs/images'), join(pub, 'images'), { recursive: true });

	const full = [];
	for (const p of pages) {
		const text = withoutTitle(readFileSync(join(root, p.src), 'utf8'));
		// "Edit this page" opens the source file: the page here is generated.
		const front = `---\ntitle: ${JSON.stringify(p.title)}\ndescription: ${JSON.stringify(p.description)}\neditUrl: ${JSON.stringify(`${repo}/edit/main/${p.src}`)}\n---\n\n`;
		writeFileSync(join(content, `${p.slug}.md`), front + rewrite(text, p.src, forSite));
		const agent = `# ${p.title}\n\n> ${p.description}\n\n${rewrite(text, p.src, forAgents)}`;
		writeFileSync(join(pub, mdName(p)), agent);
		full.push(agent);
	}
	// Pages dropped from pages.mjs. Stale files are removed one by one rather
	// than the folder recreated, so a running dev server never sees it empty.
	const keep = new Set(pages.map((p) => `${p.slug}.md`));
	for (const f of readdirSync(content)) if (!keep.has(f)) rmSync(join(content, f));

	const index = [
		'# Aquifer',
		'',
		'> A self-hosted load balancer for agent and API traffic: it stores excess work durably, paces it into the target API at the rate the target asks for, keeps tenants fair, and returns results synchronously, over a WebSocket, or by webhook.',
		'',
		`Each page below is plain Markdown. Fetch only the ones you need; ${site}${base}/llms-full.txt has all of them in one file.`,
		'',
		...groups.flatMap((g) => [
			`## ${g.label}`,
			'',
			...g.pages.map((p) => `- [${p.title}](${site}${base}/${mdName(p)}): ${p.description}`),
			'',
		]),
		'## Optional',
		'',
		`- [Source code](${repo})`,
		`- [Aquifer pilot skill](${repo}/blob/main/skills/aquifer-pilot/SKILL.md): evaluates Aquifer against your own workload in a shadow run`,
		'- [L8 specification](https://rjpruitt16.github.io/l8-protocol/)',
		'',
	].join('\n');
	writeFileSync(join(pub, 'llms.txt'), index);
	writeFileSync(join(pub, 'llms-full.txt'), full.join('\n\n---\n\n'));
	console.log(`docs: ${pages.length} pages assembled`);
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) assemble();
