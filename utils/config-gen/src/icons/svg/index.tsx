import React from 'react';

import { ReactComponent as Enabled } from './enabled.svg';
import { ReactComponent as Error } from './error.svg';
import { ReactComponent as ForwardThick } from './ic-forward-thick.svg';
import { ReactComponent as Forward } from './ic-forward.svg';
import { ReactComponent as Connection } from './ic-menu-connections.svg';
import { ReactComponent as Destination } from './ic-menu-destinations.svg';
import { ReactComponent as Rudder } from './ic-menu-rudder.svg';
import { ReactComponent as Source } from './ic-menu-sources.svg';
import { ReactComponent as Transformation } from './ic-menu-transformations.svg';
import { ReactComponent as Signout } from './ic-signout.svg';
import { ReactComponent as Plus } from './plus.svg';
import { ReactComponent as Delete } from './delete.svg';
import { ReactComponent as Selected } from './selected.svg';
import { ReactComponent as Settings } from './settings.svg';
import { ReactComponent as SideArrow } from './sideArrow.svg';
import { ReactComponent as EmptyDestinations } from './emptyDestinations.svg';
import { ReactComponent as NoData } from './no-data.svg';
import { ReactComponent as NoDataGraph } from './no-data-graph.svg';
import { ReactComponent as LogoRudder } from './logo-rudder.svg';

const Svg = (props: any) => {
  switch (props.name.toLowerCase()) {
    case 'rudder':
      return <Rudder />;
    case 'connection':
      return <Connection />;
    case 'source':
      return <Source />;
    case 'destination':
      return <Destination />;
    case 'ed':
      return <EmptyDestinations />;
    case 'transformation':
      return <Transformation />;
    case 'selected':
      return <Selected />;
    case 'signout':
      return <Signout />;
    case 'forward':
      return <Forward />;
    case 'forward-thick':
      return <ForwardThick />;
    case 'settings':
      return <Settings />;
    case 'side-arrow':
      return <SideArrow />;
    case 'plus':
      return <Plus />;
    case 'error':
      return <Error />;
    case 'enabled':
      return <Enabled />;
    case 'no-data':
      return <NoData />;
    case 'no-data-graph':
      return <NoDataGraph />;
    case 'logo-rudder':
      return <LogoRudder />;
    case 'delete':
      return <Delete />;
    default:
      break;
  }
  return <div />;
};

export default Svg;
