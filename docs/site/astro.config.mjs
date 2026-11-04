import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { assemble, sources } from './content.mjs';
import { base, groups, repo, site } from './pages.mjs';

// Assembles the pages before every build and dev server; in dev, again
// whenever a source Markdown file changes.
const content = {
	name: 'aquifer-content',
	hooks: {
		'astro:config:setup': () => assemble(),
		'astro:server:setup': ({ server }) => {
			server.watcher.add(sources);
			server.watcher.on('change', (file) => {
				if (sources.includes(file)) assemble();
			});
		},
	},
};

export default defineConfig({
	site,
	base,
	integrations: [
		content,
		starlight({
			title: 'Aquifer',
			description: 'A self-hosted load balancer for agent and API traffic.',
			social: [{ icon: 'github', label: 'GitHub', href: repo }],
			sidebar: [
				...groups.map((g) => ({
					label: g.label,
					items: g.pages.map((p) => ({ label: p.title, link: p.slug === 'index' ? '/' : `/${p.slug}/` })),
				})),
				{
					label: 'For agents',
					items: [
						{ label: 'llms.txt', link: '/llms.txt', attrs: { target: '_self' } },
						{ label: 'llms-full.txt', link: '/llms-full.txt', attrs: { target: '_self' } },
					],
				},
			],
		}),
	],
});
