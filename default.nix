let
  pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/99de747e114427ccc5b31740a6f35bb59f52c2b0.tar.gz") {};
  system_packages = builtins.attrValues {
    inherit (pkgs) quarto ;
  };
 pypkgs = builtins.attrValues {
  inherit (pkgs.python311Packages) beautifulsoup4 polars plotnine;
};
in
  pkgs.mkShell {
    buildInputs = [ system_packages pypkgs ];
  }