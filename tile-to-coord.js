function tileToLngLat(tileX, tileY, tilesPerSide = 2048) {
    const xNorm = tileX / tilesPerSide;
    const yNorm = tileY / tilesPerSide;

    const lng = xNorm * 360 - 180;
    const lat = Math.atan(Math.sinh(Math.PI * (1 - 2 * yNorm))) * 180 / Math.PI;

    return [lng, lat];
}


let argv = Bun.argv.slice(2);
let coord = tileToLngLat(argv[0], argv[1]);
console.log(`https://wplace.live/?lat=${coord[1]}&lng=${coord[0]}&zoom=15`);