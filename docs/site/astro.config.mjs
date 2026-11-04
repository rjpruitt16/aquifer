import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { base, groups, repo, site } from './pages.mjs';

export default defineConfig({
	site,
	base,
	integrations: [
		starlight({
			title: 'Aquifer',
			description: 'A self-hosted load balancer for agent and API traffic.',
			social: [{ icon: 'github', label: 'GitHub', href: repo }],
			editLink: { baseUrl: `${repo}/edit/main/` },
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
