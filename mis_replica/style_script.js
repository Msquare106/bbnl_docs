let style_html = `
.main-container{
    @apply w-screen h-screen bg-gradient-to-b from-[rgba(0,179,255,0.762)] to-[transparent];
}

.side-bar{
    @apply w-[260px] h-full flex flex-col;
    @apply bg-gradient-to-t from-[rgba(0,179,255,0.762)] to-[transparent];
    @apply shadow-[4px_3px_16px_rgba(0,0,0,0.5)];
    @apply transition-transform ease-in-out duration-200;
}

.site-title{
    @apply text-[white] text-[24px] text-center p-2 italic;
    @apply [text-shadow:2px_2px_3px_black];
}

.sidebar-toggle{
    @apply w-full px-2 flex items-center justify-center bg-[rgba(255,255,255,0.6)] cursor-pointer;
    @apply overflow-hidden shadow-[-6px_4px_6px_rgba(0,0,0,0.4)];
    @apply origin-top-left transition-all ease-in-out duration-200 hover:py-1;
}

.sidebar-toggle p,
.sidebar-toggle i{
    @apply transition-all duration-200 ease-in-out;
}

.side-bar .nav-options{
    @apply mt-2 flex flex-col gap-2;
}

.side-bar .nav-options a{
    @apply px-4 py-1 bg-gradient-to-l from-[transparent] to-[rgba(255,255,255,0.6)];
    @apply shadow-[-6px_4px_6px_rgba(0,0,0,0.4)];
    @apply transition-all duration-200 ease-in-out origin-left hover:scale-[1.15];
}

.content-container .content{
    @apply absolute w-full h-full;
}

.show-content{
    @apply opacity-[1] translate-y-0;
}

.hide-content{
    @apply opacity-[0.4] translate-y-full;
}

.content-container .content h1{
    @apply bg-[rgba(0,45,134,0.5)] text-[white] text-[28px] text-center [text-shadow:2px_2px_3px_black];
    @apply shadow-[4px_4px_6px_rgba(0,0,0,0.4)];
}

#svg2{
    @apply drop-shadow-[2px_2px_3px_black] min-w-[612px] min-h-[696px];
}

#svg2 .state{
    @apply cursor-pointer fill-[rgba(255,255,255,0.5)];
    @apply transition-all duration-200 ease-in-out;
    @apply hover:fill-[rgb(0,183,255)] hover:drop-shadow-[0_0_8px_rgba(0,0,0,0.5)];
}

.content-container .content .map-svg .state-info{
    @apply absolute w-fit min-w-[280px] h-[180px] border-1 border-[rgba(255,255,255,0.4)];
    @apply bg-gradient-to-b from-[rgba(0,0,0,0.31)] to-[transparent];
    @apply shadow-[0_16px_32px_rgba(0,0,0,0.5)] rounded-lg backdrop-blur-[3px];
    @apply transition-transform transition-opacity duration-200 ease-in-out;
}

.content .map-svg .state-info h1{
    @apply bg-[transparent] shadow-[none] text-[20px];
}
`;
