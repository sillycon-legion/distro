{
  inputs = {
    nixpkgs.url = "nixpkgs";
  };

  outputs =
    {
      nixpkgs,
      ...
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        with pkgs;
        {
          default = mkShellNoCC {
            LIBCLANG_PATH = "${libclang.lib}/lib";
          };
        }
      );
    };
}
